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

import org.apache.spark.sql.{Dataset, SparkSession}

class EmployeeDao(spark: SparkSession, employees: Dataset[Employee], departments: Dataset[Department]) {

  import spark.implicits._

  /*
   * SELECT lastName FROM employees
   */

  def lastNames(): Dataset[String] =
    employees.map(employee => employee.lastName)

  /*
   * SELECT DISTINCT lastName FROM employees
   */

  def distinctLastNames(): Dataset[String] =
    lastNames().distinct()

  /*
   * SELECT * FROM employees WHERE lastName IN('a', 'a')
   */

  def byLastName(lastNames: String*): Dataset[Employee] =
    employees.filter(employee => lastNames.contains(employee.lastName))

  /*
   * SELECT * FROM employees WHERE lastName LIKE 'foo%'
   */

  def byLastNameLike(lastName: String): Dataset[Employee] =
    employees.filter(employee => employee.lastName.startsWith(lastName))

  /*
   * SELECT ssn, e.name AS name_e, lastName, d.name AS name_d, budget
   *  FROM employees e INNER JOIN departments d
   *  ON e.department = d.code
   */

  def withDepartment(): Dataset[(String, String, String, String, Long)] = {
    employees
      .joinWith(departments, employees("department") === departments("code"), "inner")
      .map {
        case (employee, department) =>
          (employee.ssn, employee.name, employee.lastName, department.name, department.budget)
      }
  }
}

