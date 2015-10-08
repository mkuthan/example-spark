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
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, Time}

case class WordCount(word: String, count: Int)

object WordCount {

  type WordHandler = (RDD[WordCount], Time) => Unit

  def count(lines: RDD[String]): RDD[WordCount] = count(lines, Set())

  def count(lines: RDD[String], stopWords: Set[String]): RDD[WordCount] = {
    val words = prepareWords(lines, stopWords)

    val wordCounts = words.map(word => (word, 1)).reduceByKey(_ + _).map {
      case (word: String, count: Int) => WordCount(word, count)
    }

    val sortedWordCounts = wordCounts.sortBy(_.word)

    sortedWordCounts
  }

  def count(lines: DStream[String],
            windowDuration: Duration,
            slideDuration: Duration)
           (handler: WordHandler): Unit = count(lines, windowDuration, slideDuration, Set())(handler)

  def count(lines: DStream[String],
            windowDuration: Duration,
            slideDuration: Duration,
            stopWords: Set[String])
           (handler: WordHandler): Unit = {
    val words = lines.transform(prepareWords(_, stopWords))

    val wordCounts = words.map(x => (x, 1)).reduceByKeyAndWindow(_ + _, _ - _, windowDuration, slideDuration).map {
      case (word: String, count: Int) => WordCount(word, count)
    }

    wordCounts.foreachRDD((rdd: RDD[WordCount], time: Time) => {
      handler(rdd.sortBy(_.word), time)
    })
  }

  private def prepareWords(lines: RDD[String], stopWords: Set[String]): RDD[String] = {
    lines.flatMap(_.split("\\s"))
      .map(_.strip(",").strip(".").toLowerCase)
      .filter(!stopWords.contains(_)).filter(!_.isEmpty)
  }

}
