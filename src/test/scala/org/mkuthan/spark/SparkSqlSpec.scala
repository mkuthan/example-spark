package org.mkuthan.spark

import org.apache.spark.sql._
import org.scalatest._

trait SparkSqlSpec extends SparkSpec {
  this: Suite =>

  private var _sqlc: SQLContext = _

  def sqlc = _sqlc

  override def beforeAll(): Unit = {
    super.beforeAll()

    _sqlc = new SQLContext(sc)
  }

  override def afterAll(): Unit = {
    _sqlc = null

    super.afterAll()
  }


}
