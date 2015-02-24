package example

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions
import org.apache.spark.streaming.kafka.KafkaUtils

object SparkExample {
  def main(args: Array[String]) = {
    val zkQuorum = "localhost"
    val group = "example-spark"
    val topics = Map("example-spark" -> 1);

    val windowDuration = Minutes(10)
    val slideDuration = Seconds(2)
    val numPartitions = 2

    val conf = new SparkConf()
      .setAppName("ExampleSpark")
      .setMaster("local[2]")

    val ssc = new StreamingContext(conf, Seconds(2))
    // ssc.checkpoint("checkpoint")

    val messages = KafkaUtils.createStream(ssc, zkQuorum, group, topics).map(_._2)
    val words = messages.flatMap(_.split(" "))

    val wordCounts = words.map(word => (word, 1L))
      .reduceByKeyAndWindow(_ + _, _ - _, windowDuration, slideDuration, numPartitions)

    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
