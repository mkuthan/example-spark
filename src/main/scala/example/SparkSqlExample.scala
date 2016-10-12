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
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

/** Based on http://en.wikibooks.org/wiki/SQL_Exercises/Employee_management. */
object SparkSqlExample extends LazyLogging {

  private val master = "local[2]"
  private val appName = "example-spark"

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    val spark = SparkSession.builder.config(conf).enableHiveSupport().getOrCreate()

    val employees = loadEmployees(spark)
    val departments = loadDepartments(spark)

    val employeeDao = new EmployeeDao(spark, employees, departments)
    val departmentDao = new DepartmentDao(spark, departments, employees)

    logger.info("Select the last name of all employees")
    employeeDao.lastNames().collect().foreach(logger.info(_))

    logger.info("Select the last name of all employees, without duplicates.")
    employeeDao.distinctLastNames().collect().foreach(logger.info(_))

    logger.info("Select all the data of employees whose last name is \"Smith\".")
    employeeDao.byLastName("Smith").collect().map(_.toString) foreach (logger.info(_))

    logger.info("Select all the data of employees whose last name is \"Smith\" or \"Doe\".")
    employeeDao.byLastName("Smith", "Doe").collect().map(_.toString).foreach(logger.info(_))

    logger.info("Select all the data of employees whose last name begins with an \"S\".")
    employeeDao.byLastNameLike("S").collect().map(_.toString).foreach(logger.info(_))

    logger.info("Select the sum of all the departments' budgets.")
    logger.info(departmentDao.sumBudgets().toString)

    logger.info("Select the number of employees in each department.")
    departmentDao.numberOfEmployees().collect().map(_.toString()).foreach(logger.info(_))

    logger.info("Select all the data of employees, including each employee's department's data.")
    val employeesWithDepartments = employeeDao.withDepartment()
    employeesWithDepartments.collect().map(_.toString).foreach(logger.info(_))

  }

  private def loadDepartments(spark: SparkSession): Dataset[Department] = {
    import org.apache.spark.sql.types._
    import spark.implicits._
    spark.read
      .schema(StructType(Seq(
        StructField("code", IntegerType),
        StructField("name", StringType),
        StructField("budget", LongType))))
      .csv("src/main/resources/data/departments.txt").as[Department]
  }

  private def loadEmployees(spark: SparkSession): Dataset[Employee] = {
    import org.apache.spark.sql.types._
    import spark.implicits._
    spark.read
      .schema(StructType(Seq(
        StructField("ssn", StringType),
        StructField("name", StringType),
        StructField("lastName", StringType),
        StructField("department", IntegerType))))
      .csv("src/main/resources/data/employees.txt").as[Employee]
  }
}
