package org.mkuthan.spark

import java.nio.file.Files

import org.apache.spark.streaming._
import org.scalatest._

trait SparkStreamingSpec extends SparkSpec {
  this: Suite =>

  private var _ssc: StreamingContext = _

  def ssc = _ssc

  private var _clock: ClockWrapper = _

  def clock = _clock

  val batchDuration = Seconds(1)

  val checkpointDir = Files.createTempDirectory(this.getClass.getSimpleName)

  conf.set("spark.streaming.clock", "org.apache.spark.streaming.util.ManualClock")

  override def beforeAll(): Unit = {
    super.beforeAll()

    _ssc = new StreamingContext(sc, batchDuration)
    _ssc.checkpoint(checkpointDir.toString)

    _clock = new ClockWrapper(ssc)
  }

  override def afterAll(): Unit = {
    if (_ssc != null) {
      _ssc.stop()
    }

    System.clearProperty("spark.streaming.clock")

    super.afterAll()
  }


}
