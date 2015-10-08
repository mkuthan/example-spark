// Copyright (C) 2011-2012 the original author or authors.
// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.mkuthan.spark

import java.nio.file.Files
import org.apache.spark.ClockWrapper
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
      // TODO: check why context can't be stopped with stopGracefully = true
      _ssc.stop(stopSparkContext = false, stopGracefully = false)
      _ssc = null
    }

    super.afterAll()
  }


}
