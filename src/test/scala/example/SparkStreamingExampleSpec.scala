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

package example

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, Time}
import org.mkuthan.spark.SparkStreamingSpec
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Span}
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class SparkStreamingExampleSpec extends FlatSpec
with SparkStreamingSpec with GivenWhenThen with Matchers with Eventually {

  // default timeout for eventually trait
  implicit override val patienceConfig =
    PatienceConfig(timeout = scaled(Span(5000, Millis)))
  private val windowDuration = Seconds(4)
  private val slideDuration = Seconds(2)

  "Sample set" should "be counted" in {
    Given("streaming context is initialized")
    val lines = mutable.Queue[RDD[String]]()

    var results = ListBuffer.empty[Array[WordCount]]

    WordCount.count(ssc,
      ssc.queueStream(lines),
      windowDuration,
      slideDuration) { (wordsCount: RDD[WordCount], time: Time) =>
      results += wordsCount.collect()
    }

    ssc.start()

    When("first set of words queued")
    lines += sc.makeRDD(Seq("a", "b"))

    Then("words counted after first slide")
    advanceClock(slideDuration)
    eventually {
      results.last should equal(Array(
        WordCount("a", 1),
        WordCount("b", 1)))
    }

    When("second set of words queued")
    lines += sc.makeRDD(Seq("b", "c"))

    Then("words counted after second slide")
    advanceClock(slideDuration)
    eventually {
      results.last should equal(Array(
        WordCount("a", 1),
        WordCount("b", 2),
        WordCount("c", 1)))
    }

    When("nothing more queued")

    Then("word counted after third slide")
    advanceClock(slideDuration)
    eventually {
      results.last should equal(Array(
        WordCount("a", 0),
        WordCount("b", 1),
        WordCount("c", 1)))
    }

    When("nothing more queued")

    Then("word counted after fourth slide")
    advanceClock(slideDuration)
    eventually {
      results.last should equal(Array(
        WordCount("a", 0),
        WordCount("b", 0),
        WordCount("c", 0)))
    }
  }

}
