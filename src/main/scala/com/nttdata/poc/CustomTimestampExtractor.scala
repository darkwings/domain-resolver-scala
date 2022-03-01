package com.nttdata.poc

import com.nttdata.poc.model.Activity
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.processor.TimestampExtractor

import java.time.Instant

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
