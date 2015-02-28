package example

import org.apache.spark._
import org.scalatest._

class SparkExampleSpec extends FlatSpec with BeforeAndAfter with GivenWhenThen with Matchers {

  private val master = "local[2]"
  private val appName = "example-spark"

  private var sc: SparkContext = _

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    sc = new SparkContext(conf)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }

  "Empty set" should "be counted" in {
    Given("empty set")
    val lines = Array("")

    When("count words")
    val wordCounts = WordCount.count(sc.parallelize(lines)).collect()

    Then("empty count")
    wordCounts shouldBe empty
  }

  "Shakespeare most famous quote" should "be counted" in {
    Given("quote")
    val lines = Array("To be or not to be.", "That is the question.")

    Given("stop words")
    val stopWords = Set("the")

    When("count words")
    val wordCounts = WordCount.count(sc.parallelize(lines), stopWords).collect()

    Then("words counted")
    wordCounts should equal(Array(
      WordCount("be", 2),
      WordCount("is", 1),
      WordCount("not", 1),
      WordCount("or", 1),
      WordCount("question", 1),
      WordCount("that", 1),
      WordCount("to", 2)))
  }

}
