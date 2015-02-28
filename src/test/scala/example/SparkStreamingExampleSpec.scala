package example

import org.apache.spark._
import org.apache.spark.streaming._
import org.scalatest._
import org.apache.spark.streaming.util.ManualClock

class SparkStreamingExampleSpec extends FlatSpec with BeforeAndAfter with GivenWhenThen with Matchers {

  private val master = "local[2]"
  private val appName = "example-spark-streaming"
  private val batchDuration = Seconds(1)
  private val windowDuration = Seconds(30)
  private val slideDuration = Seconds(3)

  private var ssc: StreamingContext = _
  //private var mc: ManualClock = _

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
      .set("spark.streaming.clock", "org.apache.spark.streaming.util.ManualClock")

    ssc = new StreamingContext(conf, batchDuration)
    //mc = ssc.scheduler.clock.asInstanceOf[ManualClock]
  }

  after {
    if (ssc != null) {
      ssc.stop()
    }

    System.clearProperty("spark.streaming.clock")

    // avoid Akka rebinding
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")
  }

  "Empty set" should "be counted" in {
    Given("empty set")
    val lines = Array("")

    When("count words")
    /*
    WordCount.count(ssc.parallelize(lines), windowDuration, slideDuration) { (wordsCount: Array[WordCount], time: Time) =>
      val counts = time + ": " + wordsCount.mkString("[", ", ", "]")
      println(counts)
    }
    */

    Then("empty count")
    //wordCounts shouldBe empty
  }

}
