package example

import java.nio.file.Files

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.ConstantInputDStream
import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds => ScalatestSeconds, Span}

import scala.collection.mutable.ListBuffer

class SparkStreamingExampleSpec extends FlatSpec with BeforeAndAfter with GivenWhenThen with Matchers with Eventually {

  private val master = "local[2]"
  private val appName = "example-spark-streaming"
  private val batchDuration = Seconds(1)
  private val windowDuration = Seconds(4)
  private val slideDuration = Seconds(2)
  private val checkpointDir = Files.createTempDirectory(appName).toString

  private var sc: SparkContext = _
  private var ssc: StreamingContext = _
  //private var mc: ManualClock = _

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    //.set("spark.streaming.clock", "org.apache.spark.streaming.util.ManualClock")

    ssc = new StreamingContext(conf, batchDuration)
    ssc.checkpoint(checkpointDir)

    sc = ssc.sparkContext
    //mc = ssc.scheduler.clock.asInstanceOf[ManualClock]
  }

  after {
    if (ssc != null) {
      ssc.stop()
    }

    //System.clearProperty("spark.streaming.clock")

    // avoid Akka rebinding
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")
  }

  "Sample set" should "be counted" in {
    Given("empty set")
    val lines = Array("aaa", "bbb", "aaa", "ccc")

    var results = ListBuffer.empty[Array[WordCount]]
    When("count words")
    WordCount.count(new ConstantInputDStream(ssc, sc.parallelize(lines)), windowDuration, slideDuration) { (wordsCount: Array[WordCount], time: Time) =>
      results += wordsCount
    }

    ssc.start()

    Then("words counted")
    eventually(timeout(Span(8, ScalatestSeconds)), interval(Span(1, ScalatestSeconds))) {
      results should (
        have size 4
        )
    }
  }

}
