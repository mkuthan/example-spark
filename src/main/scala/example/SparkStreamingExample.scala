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

import java.nio.file.Files

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._

/**
 * To run this example, run Netcat server first: <code>nc -lk 9999</code>.
 */
object SparkStreamingExample extends LazyLogging {

  private val master = "local[2]"
  private val appName = "example-spark-streaming"
  private val stopWords = Set("a", "an", "the")

  private val batchDuration = Seconds(1)
  private val checkpointDir = Files.createTempDirectory(appName).toString
  private val windowDuration = Seconds(30)
  private val slideDuration = Seconds(3)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    val ssc = new StreamingContext(conf, batchDuration)
    ssc.checkpoint(checkpointDir)

    val lines = ssc.socketTextStream("localhost", 9999)
    WordCount.count(lines, windowDuration, slideDuration, stopWords) { (wordsCount: RDD[WordCount], time: Time) =>
      val counts = time + ": " + wordsCount.collect().mkString("[", ", ", "]")
      logger.info(counts)
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
