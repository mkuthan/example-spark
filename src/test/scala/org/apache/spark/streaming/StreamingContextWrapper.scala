package org.apache.spark.streaming

import org.apache.spark.util.ManualClock

class StreamingContextWrapper(ssc: StreamingContext) {
  val manualClock = ssc.scheduler.clock.asInstanceOf[ManualClock]
}
