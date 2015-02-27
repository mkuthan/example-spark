package example

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream

case class WordCount(word: String, count: Int)

object WordCount {

  def count(lines: RDD[String], stopWords: Set[String]): RDD[WordCount] = {
    val words = lines.flatMap(_.split("\\s"))
      .map(_.strip(",").strip(".").toLowerCase)
      .filter(!stopWords.contains(_))

    val wordCounts = words.map(word => (word, 1)).reduceByKey(_ + _).map {
      case (word: String, count: Int) => WordCount(word, count)
    }

    val sortedWordCounts = wordCounts.sortBy(_.count, ascending = false)

    sortedWordCounts
  }

  def count(lines: DStream[String], windowDuration: Duration, slideDuration: Duration, stopWords: Set[String]): Unit = {
    val words = lines.flatMap(_.split("\\s"))
      .map(_.strip(",").strip(".").toLowerCase)
      .filter(!stopWords.contains(_))

    val wordCounts = words.map(x => (x, 1)).reduceByKeyAndWindow(_ + _, _ - _, windowDuration, slideDuration).map {
      case (word: String, count: Int) => WordCount(word, count)
    }

    wordCounts.foreachRDD((rdd: RDD[WordCount], time: Time) => {
      val counts = time + ": " + rdd.collect().mkString("[", ", ", "]")
      println(counts)
    })
  }

}
