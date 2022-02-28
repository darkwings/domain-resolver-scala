package com.nttdata.poc

trait MonitorMBean {

  def addRebalance(): Unit

  def getRebalance: Integer

  def addDlqMessage(): Unit

  def getDlqMessages: Integer

  def addMessageProcessed(): Unit

  def getMessageProcessed: Long

  def getEnrichedThroughputMessagePerSecond: Long

  def getUptimeSeconds: Long
}
