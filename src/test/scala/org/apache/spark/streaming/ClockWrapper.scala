package org.apache.spark.streaming

import org.apache.spark.streaming.util.ManualClock

class ClockWrapper(ssc: StreamingContext) {

  private val manualClock = ssc.scheduler.clock.asInstanceOf[ManualClock]

  def getTimeMillis: Long = manualClock.currentTime()

  def setTime(timeToSet: Long) = manualClock.setTime(timeToSet)

  def advance(timeToAdd: Long) = manualClock.addToTime(timeToAdd)

  def waitTillTime(targetTime: Long): Long = manualClock.waitTillTime(targetTime)

}
