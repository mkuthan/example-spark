package org.mkuthan.spark

import org.apache.spark.sql.hive.HiveContext
import org.scalatest._

trait SparkSqlSpec extends SparkSpec {
  this: Suite =>

  private var _sqlc: HiveContext = _

  def sqlc = _sqlc

  override def beforeAll(): Unit = {
    super.beforeAll()

    _sqlc = new HiveContext(sc)
  }

  override def afterAll(): Unit = {
    _sqlc = null

    super.afterAll()
  }

}
