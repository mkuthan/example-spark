package example

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark._

object SparkExample extends LazyLogging {

  private val master = "local[2]"
  private val appName = "example-spark"

  def main(args: Array[String]) = {

    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    val sc = new SparkContext(conf)

    val lines = sc.textFile("hdfs://zeus/tmp/metaitem_rules.csv")
    val count = lines.count()

    logger.info(s"Count: $count")
  }
}
