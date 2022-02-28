package com.nttdata.poc

import com.nttdata.poc.model.{Activity, ActivityEnriched, Domain, JsonSerDes}
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams.State.REBALANCING
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD
import org.apache.kafka.streams.processor.api.{Processor, ProcessorContext, Record}
import org.apache.kafka.streams.processor.{Cancellable, PunctuationType, Punctuator}
import org.apache.kafka.streams.state.{KeyValueStore, Stores}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import sttp.client3.{HttpURLConnectionBackend, UriContext, basicRequest}

import java.time.{Duration, Instant}
import java.util
import java.util.Properties

class DomainResolverProcessorApi(bootstrapServers: String, sourceTopic: String, destTopic: String,
                                 stateDir: String, apiKey: String, periodTtl: Long, domainTtlMillis: Long) {

  def start(): Unit = {

    val streams = new KafkaStreams(createTopology(), properties())
    streams.setUncaughtExceptionHandler(_ => REPLACE_THREAD)
    streams.setStateListener((newState: KafkaStreams.State, oldState: KafkaStreams.State) => {
      if (newState eq REBALANCING) {
        // log.info("Rebalancing")
      }
    })
    streams.start()

    sys.ShutdownHookThread {streams.close()}
  }

  def createTopology(): Topology = {
    val builder = new Topology

    builder.addSource("Activity",
      Serdes.String.deserializer,
      JsonSerDes.activity().deserializer(),
      sourceTopic
    )

    builder.addProcessor("Activity processor",
      () => new ActivityProcessor(apiKey, periodTtl, domainTtlMillis), "Activity")

    val topicConfigs: util.Map[String, String] = new util.HashMap[String, String]
    // topicConfigs.put("min.insync.replicas", "2"); // TODO
    // topicConfigs.put(RETENTION_MS_CONFIG, Long.toString(domainTtlMillis)); // TODO

    // TODO il problema qui è che la chiave dello store non è la partition key, per cui
    //    lo store non è partizionato correttamente, tutte le istanze potrebbero avere gli
    //    stessi dati. Alternativa, fare qualcosa del genere, ma a quel punto meglio DSL e selectKey
    //    topology.addSink(...); write to new topic for repartitioning
    //    topology.addSource(...); // read from repartition topic
    val storeBuilder = Stores.keyValueStoreBuilder(
      Stores.persistentKeyValueStore("domain-store"),
      Serdes.String,
      JsonSerDes.domain()).withLoggingEnabled(topicConfigs)

    builder.addStateStore(storeBuilder, "Activity processor")

    builder.addSink("Activity enriched Sink", destTopic, Serdes.String.serializer,
      JsonSerDes.activityEnriched().serializer,
      "Activity processor")
    builder
  }

  def properties(): Properties = {
    val properties = new Properties();
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDir)
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "domain-resolution-processor-api")
    properties
  }
}

class ActivityProcessor(apiKey:String, periodTtl: Long, domainTtlMillis: Long)
  extends Processor[String, Activity, String, ActivityEnriched] {

  val payload: String = "{\n" + "  \"client\": {\n" + "    \"clientId\": \"cmp\",\n" + "    \"clientVersion\": \"1.5.2\"\n" + "  },\n" + "  \"threatInfo\": {\n" + "    \"threatTypes\": [\n" + "      \"MALWARE\",\n" + "      \"SOCIAL_ENGINEERING\",\n" + "      \"THREAT_TYPE_UNSPECIFIED\"\n" + "    ],\n" + "    \"platformTypes\": [\n" + "      \"ALL_PLATFORMS\"\n" + "    ],\n" + "    \"threatEntryTypes\": [\n" + "      \"URL\"\n" + "    ],\n" + "    \"threatEntries\": [\n" + "      {\n" + "        \"url\": \"%DOMAIN%\"\n" + "      }\n" + "    ]\n" + "  }\n" + "}";

  var context: ProcessorContext[String, ActivityEnriched] = _
  var kvStore: KeyValueStore[String, Domain] = _
  var punctuator: Cancellable = _

  override def init(c: ProcessorContext[String, ActivityEnriched]): Unit = {
    context = c
    kvStore = context.getStateStore("domain-store")
    punctuator = this.context.schedule(Duration.ofMillis(periodTtl),
      PunctuationType.WALL_CLOCK_TIME, new ActivityPunctuator())
  }

  override def process(record: Record[String, Activity]): Unit = {

    val activity = record.value()
    val domainStr = activity.domain
    val opt = Option(kvStore.get(domainStr))
    opt match {
      case None =>
        try {
          val p = payload.replaceAll("%DOMAIN%", domainStr)
          val request = basicRequest
            .body(p)
            .acceptEncoding("application/json")
            .contentType("application/json")
            .post(uri"https://safebrowsing.googleapis.com/v4/threatMatches:find?key=$apiKey")
          val response = request.send(HttpURLConnectionBackend())
          val suspect = response.body match {
            case Right(c) => !c.startsWith("{}")
            case _ => true
          }
          kvStore.put(domainStr, Domain(activity.domain, suspect))
        }
        catch {
          case _: Throwable => kvStore.put(domainStr, Domain(activity.domain, suspect = true))
        }
      case Some(v) =>
        val activityEnriched = ActivityEnriched(activity, v.suspect)
        context.forward(new Record[String, ActivityEnriched](record.key, activityEnriched,
          record.timestamp))
    }
  }

  class ActivityPunctuator extends Punctuator {
    override def punctuate(timestamp: Long): Unit = {
      try {
        val iter = kvStore.all
        try while ( {iter.hasNext}) {
          val entry = iter.next
          val lastDomain = entry.value
          if (lastDomain != null) {
            val lastUpdated = Instant.parse(lastDomain.timestamp)
            val millisFromLastUpdate = Duration.between(lastUpdated, Instant.now).toMillis
            if (millisFromLastUpdate >= domainTtlMillis) kvStore.delete(entry.key)
          }
        }
        finally if (iter != null) iter.close()
      }
    }
  }

  override def close(): Unit = {
    punctuator.cancel()
  }
}

//object Runner {
//
//  def main(a: Array[String]): Unit = {
//    val bootstrapServers = a(0)
//    val sourceTopic = a(1)
//    val destTopic = a(2)
//    val stateDir = a(3)
//    val apiKey = a(4)
//    val periodTtl = a(5).toLong
//    val ttl = a(6).toLong
//    val resolver = new DomainResolverProcessorApi(bootstrapServers, sourceTopic, destTopic,
//      stateDir, apiKey, periodTtl, ttl)
//    resolver.start()
//  }
//}