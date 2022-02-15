package com.nttdata.poc

import com.nttdata.poc.model.{Activity, ActivityEnriched, Domain, JsonSerDes}
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams.State.REBALANCING
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD
import org.apache.kafka.streams.processor.api.{Processor, ProcessorContext, Record}
import org.apache.kafka.streams.state.{KeyValueStore, Stores}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import sttp.client3.{HttpURLConnectionBackend, UriContext, basicRequest}

import java.util.Properties

class DomainResolverProcessorApi(bootstrapServers: String,
                                 sourceTopic: String,
                                 destTopic: String,
                                 stateDir: String,
                                 apiKey: String) {

  val payload: String = "{\n" + "  \"client\": {\n" + "    \"clientId\": \"cmp\",\n" + "    \"clientVersion\": \"1.5.2\"\n" + "  },\n" + "  \"threatInfo\": {\n" + "    \"threatTypes\": [\n" + "      \"MALWARE\",\n" + "      \"SOCIAL_ENGINEERING\",\n" + "      \"THREAT_TYPE_UNSPECIFIED\"\n" + "    ],\n" + "    \"platformTypes\": [\n" + "      \"ALL_PLATFORMS\"\n" + "    ],\n" + "    \"threatEntryTypes\": [\n" + "      \"URL\"\n" + "    ],\n" + "    \"threatEntries\": [\n" + "      {\n" + "        \"url\": \"%DOMAIN%\"\n" + "      }\n" + "    ]\n" + "  }\n" + "}";

  def start(): Unit = {

    val streams = new KafkaStreams(createTopology(), properties())
    streams.setUncaughtExceptionHandler(_ => REPLACE_THREAD)
    streams.setStateListener((newState: KafkaStreams.State, oldState: KafkaStreams.State) => {
      if (newState eq REBALANCING) {
        // log.info("Rebalancing")
      }
    })
    streams.start()
  }

  def createTopology(): Topology = {
    val builder = new Topology

    builder.addSource("Activity",
      Serdes.String.deserializer,
      JsonSerDes.activity().deserializer(),
      sourceTopic
    )

    builder.addProcessor("Activity processor",
      () => new ActivityProcessor(), "Activity")

    val storeBuilder = Stores.keyValueStoreBuilder(
      Stores.persistentKeyValueStore("domain-store"),
      Serdes.String,
      JsonSerDes.domain())

    builder.addStateStore(storeBuilder, "Activity processor")

    builder.addSink("Activity enriched Sink", destTopic, Serdes.String.serializer,
      JsonSerDes.activityEnriched().serializer,
      "Activity processor")
    builder
  }

  class ActivityProcessor extends Processor[String, Activity, String, ActivityEnriched] {
    var context: ProcessorContext[String, ActivityEnriched] = _
    var kvStore: KeyValueStore[String, Domain] = _

    override def init(c: ProcessorContext[String, ActivityEnriched]): Unit = {
      context = c
      kvStore = context.getStateStore("domain-store")
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
            val backend = HttpURLConnectionBackend()
            val response = request.send(backend)
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

    def get(k: String): Option[Domain] = {
      val s = kvStore.get(k)
      Option(s)
    }
  }

  def properties(): Properties = {
    val properties = new Properties();
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDir)
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "domain-resolution-processor-api")
    properties
  }
}

object Runner {

  def main(a: Array[String]): Unit = {
    val bootstrapServers = a(0)
    val sourceTopic = a(1)
    val destTopic = a(2)
    val domainsTopic = a(3) // Non serve, la gestione dello State Store è manuale
    val stateDir = a(4)
    val apiKey = a(5)
    val resolver = new DomainResolverProcessorApi(bootstrapServers, sourceTopic, destTopic, stateDir, apiKey)
    resolver.start()
  }
}