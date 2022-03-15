package com.nttdata.poc

import com.typesafe.scalalogging.Logger

import java.lang.management.ManagementFactory
import javax.management.{ObjectName}

class Monitor extends MonitorMBean {

  private val NANOS_TO_SECONDS_RATIO = 1_000_000_000L

  private var rebalance = 0
  private var dlqMessages = 0
  private var retryMessages = 0
  private var cacheMiss = 0
  private var messageProcessed = 0L


  private val startTime = 0L

  private var windowStart = System.nanoTime
  private var windowMessages = System.nanoTime

  override def addRebalance(): Unit = {
    rebalance += 1
  }

  override def getRebalance: Integer = rebalance

  override def addDlqMessage(): Unit = {
    dlqMessages += 1
  }

  override def getDlqMessages: Integer = dlqMessages

  override def addMessageProcessed(): Unit = {
    messageProcessed += 1
    windowMessages += 1
  }

  override def getMessageProcessed: Long = messageProcessed

  override def getUptimeSeconds: Long = (System.nanoTime - startTime) / NANOS_TO_SECONDS_RATIO

  override def getEnrichedThroughputMessagePerSecond: Long = try {
    val windowSec = (System.nanoTime - windowStart) / NANOS_TO_SECONDS_RATIO
    val count = if (windowSec != 0) windowMessages / windowSec
    else 0L
    // TODO meglio, così è solo abbozzato
    windowStart = System.nanoTime // Restart the window
    windowMessages = 0
    count
  } catch {
    case _: Exception => 0L
  }

  override def addRetryMessage(): Unit = retryMessages += 1

  override def getRetryMessage: Int = retryMessages

  override def addCacheMiss(): Unit = cacheMiss += 1

  override def getCacheMiss: Int = cacheMiss
}

object Monitor {

  val logger: Logger = Logger("Monitor")

  def create(): Monitor = {
    val m = new Monitor()
    val disable = System.getProperty("disable.jmx")
    if (disable == null) try {
      val objectName = new ObjectName("com.nttdata.poc:type=basic,name=monitor")
      val server = ManagementFactory.getPlatformMBeanServer
      server.registerMBean(m, objectName)
    } catch {
      case e: Exception =>
        logger.error("Failed to register monitor MBean", e)
    }
    m
  }
}
