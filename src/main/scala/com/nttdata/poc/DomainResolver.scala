package com.nttdata.poc

import com.nttdata.poc.model.{Activity, ActivityEnriched, Domain, JsonSerDes}
import com.typesafe.scalalogging.Logger
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.KafkaStreams.State.REBALANCING
import org.apache.kafka.streams.StreamsConfig.{APPLICATION_ID_CONFIG, BOOTSTRAP_SERVERS_CONFIG, STATE_DIR_CONFIG}
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD
import org.apache.kafka.streams.kstream.{Branched, Consumed, Joined, KStream, KTable, Materialized, Named, Predicate, Produced, ValueJoiner, ValueTransformer, ValueTransformerSupplier}
import org.apache.kafka.streams.processor.{Cancellable, ProcessorContext, PunctuationType, Punctuator, TimestampExtractor}
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, Topology}
import pureconfig.ConfigSource
import pureconfig.generic.auto._
import sttp.client3.{HttpURLConnectionBackend, UriContext, basicRequest}

import java.time.{Duration, Instant}
import java.util
import java.util.{HashMap, Map, Properties}

class CustomTimestampExtractor extends TimestampExtractor {
  override def extract(record: ConsumerRecord[AnyRef, AnyRef], partitionTime: Long): Long = {
    val a = record.value().asInstanceOf[Activity]
    if (a != null && a.timestamp != null) {
      Instant.parse(a.timestamp).toEpochMilli
    }
    else {
      partitionTime
    }
  }
}

sealed trait IntermediateResultState

object OK extends IntermediateResultState

object KO extends IntermediateResultState

case class IntermediateResult[A, L](enrichedData: A, lookup: L, state: IntermediateResultState)

class DomainResolver(conf: ServiceConf) {

  val logger: Logger = Logger("DomainResolver")
  val STORE_NAME: String = "domain-store"


  def start(): Unit = {
    val streams = new KafkaStreams(createTopology(conf), properties(conf))
    streams.setUncaughtExceptionHandler(_ => REPLACE_THREAD)
    streams.setStateListener((newState: KafkaStreams.State, oldState: KafkaStreams.State) => {
      logger.info("New state is {} (from {})", newState, oldState)
      if (newState eq REBALANCING) {
        logger.info("Started rebalancing")
      }
    })
    streams.start()

    sys.ShutdownHookThread {
      streams.close()
    }
  }



  def supplyValueTransformer(conf: ServiceConf): ValueTransformerSupplier[ActivityEnriched, IntermediateResult[ActivityEnriched, Domain]] =
    () => new ExtValueTransformer(conf)

  def createTopology(conf: ServiceConf): Topology = {
    val monitor = Monitor.create()

    val builder = new StreamsBuilder
    val activityStream = builder.stream(conf.topics.source,
      Consumed.`with`(Serdes.String, JsonSerDes.activity())
        .withTimestampExtractor(new CustomTimestampExtractor))

    val activityBranched = activityStream.split(Named.as("Activity-"))
      .branch(enrichOff, Branched.as("dlq"))
      .branch(enrichTriggered, Branched.as("enrich-triggered"))
      .defaultBranch(Branched.as("undefined"));

    // ======================================================================================
    // Se il dominio non è presente nel dato entrante,
    // mando il dato in DLQ
    activityBranched.get("Activity-dlq")
      .peek((_, _) => monitor.addDlqMessage())
      .to(conf.topics.dlq, Produced.`with`(Serdes.String, JsonSerDes.activity()))

    // ======================================================================================
    // Se il dominio è presente, provo a verificare se è sospetto o no con una join
    // con la tabella domains.
    // Devo cambiare chiave, in modo da poter fare una join (co-partitioned) con la tabella
    // dei domini.
    val activities: KStream[String, Activity] = activityBranched.get("Activity-enrich-triggered")
      .selectKey(lookupKey)

    val topicConfigs = new util.HashMap[String, String]
    topicConfigs.put("min.insync.replicas", Integer.toString(conf.topics.minInsyncReplicas))

    val domainTable: KTable[String, Domain] = builder
      .table(conf.topics.lookup, Consumed.`with`(Serdes.String, JsonSerDes.domain()),
        Materialized.as[String, Domain, KeyValueStore[Bytes, Array[Byte]]](STORE_NAME)
          .withLoggingEnabled(topicConfigs))


    // TODO rendere generico
    val vj:ValueJoiner[Activity, Domain, ActivityEnriched] = (a, d) => Option(d) match {
      case Some(d) => ActivityEnriched(a, d.suspect)
      case None => ActivityEnriched(a)
    }
    val activityEnriched: KStream[String, ActivityEnriched] = activities.leftJoin(domainTable, vj,
      Joined.`with`(Serdes.String, JsonSerDes.activity(), JsonSerDes.domain()));

    // Controllo enrichment effettuato
    // Se nello stream enriched ho suspect=null, devo fare una query sul sistema esterno
    // e poi scrivere sul topic di lookup per aggiornare lo State Store

    val enrichBranches = activityEnriched.split(Named.as("Enrich-"))
      .branch(enrichAvailable, Branched.as("done"))
      .branch(enrichRequired, Branched.as("to-be-done"))
      .defaultBranch(Branched.as("def"))

    // Caso 1
    // Enrichment presente, posso scrivere direttamente sul topic destinazione,
    // ma devo fare una rekey per riportare la chiave al valore iniziale "activityId"
    enrichBranches.get("Enrich-done").selectKey(this.enrichedStreamKey)
      .peek((_, _) => monitor.addMessageProcessed())
      .to(conf.topics.dest, Produced.`with`(Serdes.String, JsonSerDes.activityEnriched()))

    // Caso 2
    // Informazioni di enrichment non in cache
    // (il dominio non è definito sullo state store) -> interrogo il sistema
    // esterno e poi salvo il dato sul topic che alimenta lo state store

    val okPredicate: Predicate[String, IntermediateResult[_, _]] = (_, v) => v.state == OK
    val koPredicate: Predicate[String, IntermediateResult[_, _]] = (_, v) => v.state == KO
    val enrichNeededBranches = enrichBranches.get("Enrich-to-be-done")
      .transformValues(supplyValueTransformer(conf), STORE_NAME)
      .split(Named.as("Client-"))
      .branch(okPredicate, Branched.as("OK"))
      .branch(koPredicate, Branched.as("KO"))
      .defaultBranch(Branched.as("cd"));

    // 2a:
    // se l'interrogazione al sistema esterno fallisce, il record originale va in DLQ
    enrichNeededBranches.get("Client-KO")
      .mapValues((v: IntermediateResult[ActivityEnriched, Domain]) => v.enrichedData.activity)
      .selectKey((_: String, v: Activity) => v.activityId)
      .peek((_, _) => monitor.addDlqMessage())
      .to(conf.topics.dlq, Produced.`with`(Serdes.String, JsonSerDes.activity()))

    // 2b:
    // interrogazione OK di sistema esterno.
    val branchOk = enrichNeededBranches.get("Client-OK")

    // 2b-1
    // L'informazione di arricchimento va sullo state store (attraverso il topic dedicato)
    branchOk.mapValues(v => v.lookup).to(conf.topics.lookup,
      Produced.`with`(Serdes.String, JsonSerDes.domain()))

    // 2b-2
    // Il dato arricchito va sul topic destinazione per i downstream consumer
    branchOk.mapValues(v => v.enrichedData)
      .selectKey(enrichedStreamKey)
      .peek((_, _) => monitor.addMessageProcessed())
      .to(conf.topics.dest, Produced.`with`(Serdes.String(), JsonSerDes.activityEnriched()))

    builder.build()
  }

  protected def enrichOff(s: String, a: Activity): Boolean =
    a.domain == null || a.domain.trim.isEmpty

  protected def enrichTriggered(s: String, a: Activity): Boolean =
    a.domain != null && a.domain.trim.nonEmpty

  protected def lookupKey(s: String, activity: Activity): String = activity.domain

  protected def enrichAvailable(s: String, activityEnriched: ActivityEnriched): Boolean =
    activityEnriched.lookupComplete

  protected def enrichRequired(s: String, activityEnriched: ActivityEnriched): Boolean =
    !activityEnriched.lookupComplete

  protected def enrichedStreamKey(s: String, activityEnriched: ActivityEnriched): String = activityEnriched.activity.activityId

  def properties(conf: ServiceConf): Properties = {
    val properties = new Properties();
    properties.put(BOOTSTRAP_SERVERS_CONFIG, conf.bootstrapServers)
    properties.put(STATE_DIR_CONFIG, conf.stateStore.dir)
    properties.put(APPLICATION_ID_CONFIG, conf.applicationId)
    properties
  }
}

class ExtValueTransformer(conf: ServiceConf) extends ValueTransformer[ActivityEnriched, IntermediateResult[ActivityEnriched, Domain]] {

  val payload: String = "{\n" + "  \"client\": {\n" + "    \"clientId\": \"cmp\",\n" + "    \"clientVersion\": \"1.5.2\"\n" + "  },\n" + "  \"threatInfo\": {\n" + "    \"threatTypes\": [\n" + "      \"MALWARE\",\n" + "      \"SOCIAL_ENGINEERING\",\n" + "      \"THREAT_TYPE_UNSPECIFIED\"\n" + "    ],\n" + "    \"platformTypes\": [\n" + "      \"ALL_PLATFORMS\"\n" + "    ],\n" + "    \"threatEntryTypes\": [\n" + "      \"URL\"\n" + "    ],\n" + "    \"threatEntries\": [\n" + "      {\n" + "        \"url\": \"%DOMAIN%\"\n" + "      }\n" + "    ]\n" + "  }\n" + "}";

  var context: ProcessorContext = _
  var kvStore: KeyValueStore[String, Domain] = _
  var punctuator: Cancellable = _
  val apiKey: String = System.getProperty("api.key")

  override def init(c: ProcessorContext): Unit = {
    context = c
    kvStore = context.getStateStore("domain-store")
    punctuator = this.context.schedule(Duration.ofMillis(conf.stateStore.ttlCheckPeriodMs),
      PunctuationType.WALL_CLOCK_TIME, new ActivityPunctuator())
  }

  override def transform(activityE: ActivityEnriched): IntermediateResult[ActivityEnriched, Domain] = {
    val domainStr = activityE.activity.domain
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
          IntermediateResult(activityE, Domain(domainStr, suspect, Instant.now().toString), OK)
        }
        catch {
          case _: Throwable =>
            IntermediateResult(activityE, Domain(domainStr, suspect = true, Instant.now().toString), KO)
        }
      case Some(v) =>
        IntermediateResult(activityE, v, OK)
    }
  }

  override def close(): Unit = {
    punctuator.cancel()
  }

  class ActivityPunctuator extends Punctuator {
    override def punctuate(timestamp: Long): Unit = {
      try {
        val iter = kvStore.all
        try while ( {
          iter.hasNext
        }) {
          val entry = iter.next
          val lastDomain = entry.value
          if (lastDomain != null) {
            val lastUpdated = Instant.parse(lastDomain.timestamp)
            val millisFromLastUpdate = Duration.between(lastUpdated, Instant.now).toMillis
            if (millisFromLastUpdate >= conf.stateStore.ttlMs) kvStore.delete(entry.key)
          }
        }
        finally if (iter != null) iter.close()
      }
    }
  }
}

object Runner {
  def main(a: Array[String]) = {
    val otherAppSource = ConfigSource.file(a(0))
    val res = otherAppSource.load[ServiceConf]
    res match {
      case Right(c) => new DomainResolver(c).start()
      case Left(e) => println(e)
    }
  }
}
