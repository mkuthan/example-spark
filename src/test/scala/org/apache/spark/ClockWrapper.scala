package org.apache.spark

import org.apache.spark.streaming.{StreamingContext, StreamingContextWrapper}

class ClockWrapper(ssc: StreamingContext) {

  private val manualClock = new StreamingContextWrapper(ssc).manualClock

  def getTimeMillis: Long = manualClock.getTimeMillis()

  def setTime(timeToSet: Long) = manualClock.setTime(timeToSet)

  def advance(timeToAdd: Long) = manualClock.advance(timeToAdd)

  def waitTillTime(targetTime: Long): Long = manualClock.waitTillTime(targetTime)

}
