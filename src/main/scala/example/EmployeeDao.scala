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
import org.apache.spark.sql.{Row, SQLContext}

/** Notice: Variable binding is vulnerable for SQL injection. */
class EmployeeDao(sqlc: SQLContext) {

  import example.EmployeeDao._

  def lastNames(): RDD[String] =
    sqlc
      .sql("SELECT lastName FROM employees")
      .map(row => row.getString(0))

  def distinctLastNames(): RDD[String] =
    sqlc
      .sql("SELECT DISTINCT lastName FROM employees")
      .map(row => row.getString(0))

  def byLastName(lastNames: String*): RDD[Employee] =
    sqlc
      .sql(s"SELECT * FROM employees WHERE lastName IN(${lastNames.mkString("'", "', '", "'")})")
      .map(toEmployee)

  def byLastNameLike(lastName: String): RDD[Employee] =
    sqlc
      .sql(s"SELECT * FROM employees WHERE lastName LIKE '$lastName%'")
      .map(toEmployee)

  def withDepartment(): RDD[(String, String, String, String, Int)] = {
    val sql =
      """
        |SELECT ssn, e.name AS name_e, lastName, d.name AS name_d, budget
        | FROM employees e INNER JOIN departments d
        | ON e.department = d.code
      """.stripMargin

    sqlc
      .sql(sql)
      .map(row => (row.getString(0), row.getString(1), row.getString(2), row.getString(3), row.getInt(4)))
  }
}

object EmployeeDao {
  private def toEmployee(row: Row): Employee =
    Employee(row.getString(0), row.getString(1), row.getString(2), row.getInt(3))
}
