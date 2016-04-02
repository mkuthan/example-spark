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

import org.mkuthan.spark.SparkSpec
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}

class SparkExampleSpec extends FlatSpec with SparkSpec with GivenWhenThen with Matchers {

  "Empty set" should "be counted" in {
    Given("empty set")
    val lines = Array("")

    When("count words")
    val wordCounts = WordCount.count(sc, sc.parallelize(lines)).collect()

    Then("empty count")
    wordCounts shouldBe empty
  }

  "Shakespeare most famous quote" should "be counted" in {
    Given("quote")
    val lines = Array("To be or not to be.", "That is the question.")

    Given("stop words")
    val stopWords = Set("the")

    When("count words")
    val wordCounts = WordCount.count(sc, sc.parallelize(lines), stopWords).collect()

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
