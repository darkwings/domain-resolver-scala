package com.nttdata.poc

import com.typesafe.scalalogging.Logger
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.streams.processor.StateRestoreListener

class CustomRestoreListener extends StateRestoreListener {
  val logger: Logger = Logger("CustomRestoreListener")

  override def onRestoreStart(tp: TopicPartition,
                              storeName: String, startingOffset: Long, endingOffset: Long): Unit = {
    logger.warn("Restore started on store {} (tp {})", storeName, tp)
  }

  override def onBatchRestored(tp: TopicPartition,
                               storeName: String, batchEndOffset: Long, numRestored: Long): Unit = {
    // does nothing
  }

  override def onRestoreEnd(tp: TopicPartition,
                            storeName: String, totalRestored: Long): Unit = {
    logger.warn("Restore ended on store {} (tp {}). Restored {}", storeName, tp, totalRestored)
  }
}