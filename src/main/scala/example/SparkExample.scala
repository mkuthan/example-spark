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

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark._

object SparkExample extends LazyLogging {

  private val master = "local[2]"
  private val appName = "example-spark"
  private val stopWords = Set("a", "an", "the")

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    val sc = new SparkContext(conf)

    val lines = sc.textFile("src/main/resources/data/words.txt")
    val wordsCount = WordCount.count(sc, lines, stopWords)

    val counts = wordsCount.collect().mkString("[", ", ", "]")
    logger.info(counts)
  }
}
