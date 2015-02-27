package example

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.scalatest.{Matchers, GivenWhenThen, BeforeAndAfter, FunSuite}

class SparkExampleSpec extends FunSuite with BeforeAndAfter with GivenWhenThen with Matchers {

  private val master = "local[2]"
  private val appName = "example-spark"

  private var sc: SparkContext = _

  before {
    sc = new SparkContext(master, appName)
  }

  after {
    if (sc != null) {
      sc.stop()
    }

    // avoid Akka rebinding
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")
  }

  test("Should pass") {
    Given("few lines of text")
    val lines = Array("To be or not to be.", "That is the question.")

    Given("stop words")
    val stopWords = Set("the")

    When("count words")
    val wordCounts = WordCount.count(sc.parallelize(lines), stopWords).collect()

    Then("words counted")
    wordCounts should (
      have size 7 and
        contain inOrderOnly(
        WordCount("be", 2),
        WordCount("to", 2),
        WordCount("is", 1),
        WordCount("question", 1),
        WordCount("not", 1),
        WordCount("that", 1),
        WordCount("or", 1))
      )
  }

}
