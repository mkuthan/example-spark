package example

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.mkuthan.spark.SparkStreamingSpec
import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Span}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class SparkStreamingExampleSpec extends FlatSpec with SparkStreamingSpec with GivenWhenThen with Matchers with Eventually {

  private val windowDuration = Seconds(4)
  private val slideDuration = Seconds(2)

  // default timeout for eventually trait
  implicit override val patienceConfig =
    PatienceConfig(timeout = scaled(Span(1500, Millis)))

  "Sample set" should "be counted" in {
    Given("streaming context is initialized")
    val lines = mutable.Queue[RDD[String]]()

    var results = ListBuffer.empty[Array[WordCount]]

    WordCount.count(ssc, ssc.queueStream(lines), windowDuration, slideDuration) { (wordsCount: RDD[WordCount], time: Time) =>
      results += wordsCount.collect()
    }

    ssc.start()

    When("first set of words queued")
    lines += sc.makeRDD(Seq("a", "b"))

    Then("words counted after first slide")
    clock.advance(slideDuration.milliseconds)
    eventually {
      results.last should equal(Array(
        WordCount("a", 1),
        WordCount("b", 1)))
    }

    When("second set of words queued")
    lines += sc.makeRDD(Seq("b", "c"))

    Then("words counted after second slide")
    clock.advance(slideDuration.milliseconds)
    eventually {
      results.last should equal(Array(
        WordCount("a", 1),
        WordCount("b", 2),
        WordCount("c", 1)))
    }

    When("nothing more queued")

    Then("word counted after third slide")
    clock.advance(slideDuration.milliseconds)
    eventually {
      results.last should equal(Array(
        WordCount("a", 0),
        WordCount("b", 1),
        WordCount("c", 1)))
    }

    When("nothing more queued")

    Then("word counted after fourth slide")
    clock.advance(slideDuration.milliseconds)
    eventually {
      results.last should equal(Array(
        WordCount("a", 0),
        WordCount("b", 0),
        WordCount("c", 0)))
    }
  }

}
