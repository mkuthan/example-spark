package example

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql._

object SparkSqlExample extends LazyLogging {

  private val master = "local[2]"
  private val appName = "example-spark"

  def main(args: Array[String]) = {

    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    val sc = new SparkContext(conf)
    val sqlc = new SQLContext(sc)

    import sqlc.createSchemaRDD

    val people = sc.textFile("src/main/resources/data/people.txt")
      .map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt))

    people.registerTempTable("people")

    val teenagers = sqlc.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")
    logger.info(teenagers.toDebugString)

    teenagers.map(t => "Name: " + t(0)).collect().foreach(println)
  }

  case class Person(name: String, age: Int)

}
