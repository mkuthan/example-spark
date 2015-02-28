package example

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark._

object SparkExample extends LazyLogging {

  private val master = "local[2]"
  private val appName = "example-spark"
  private val stopWords = Set("a", "an", "the")

  def main(args: Array[String]) = {

    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    val sc = new SparkContext(conf)

    val lines = sc.textFile("src/main/resources/data")
    val counts = WordCount.count(lines, stopWords)

    logger.info(counts.collect().mkString("[", ", ", "]"))
  }
}
