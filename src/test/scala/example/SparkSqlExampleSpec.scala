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

class SparkSqlExampleSpec extends FlatSpec with SparkSpec with GivenWhenThen with Matchers {

  private val employees = Seq(
    Employee("123234877", "Michael", "Rogers", 14),
    Employee("152934485", "Anand", "Manikutty", 14),
    Employee("222364883", "Carol", "Smith", 37),
    Employee("326587417", "Joe", "Stevens", 37),
    Employee("332154719", "Mary-Anne", "Foster", 14),
    Employee("332569843", "George", "ODonnell", 77),
    Employee("546523478", "John", "Doe", 59),
    Employee("631231482", "David", "Smith", 77),
    Employee("654873219", "Zacary", "Efron", 59),
    Employee("745685214", "Eric", "Goldsmith", 59),
    Employee("845657245", "Elizabeth", "Doe", 14),
    Employee("845657246", "Kumar", "Swamy", 14)
  )
  private val departments = Seq(
    Department(14, "IT", 65000),
    Department(37, "Accounting", 15000),
    Department(59, "Human Resources", 240000),
    Department(77, "Research", 55000)
  )
  private var employeeDao: EmployeeDao = _
  private var departmentDao: DepartmentDao = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val _spark = spark
    import _spark.implicits._

    val employeesDs = employees.toDS()
    val departmentsDs = departments.toDS()

    employeeDao = new EmployeeDao(spark, employeesDs, departmentsDs)
    departmentDao = new DepartmentDao(spark, departmentsDs, employeesDs)
  }

  "The last name of all employees" should "be selected" in {
    val lastNames = employeeDao.lastNames().collect()

    lastNames should have length 12
  }

  "The last name of all employees" should "be selected without duplicates" in {
    val distinctLastNames = employeeDao.distinctLastNames().collect()

    distinctLastNames should have length 10
  }

  "The employees whose last name is 'Smith'" should "be selected" in {
    val smiths = employeeDao.byLastName("Smith").collect()

    smiths should equal(Array(
      Employee("222364883", "Carol", "Smith", 37),
      Employee("631231482", "David", "Smith", 77)
    ))
  }

  "The employees whose last name is 'Smith' or 'Doe'" should "be selected" in {
    val smithsOrDoes = employeeDao.byLastName("Smith", "Doe").collect()

    smithsOrDoes should equal(Seq(
      Employee("222364883", "Carol", "Smith", 37),
      Employee("546523478", "John", "Doe", 59),
      Employee("631231482", "David", "Smith", 77),
      Employee("845657245", "Elizabeth", "Doe", 14)
    ))
  }

  "The employees whose last name name begins with an 'S'" should "be selected" in {
    val smithsOrDoes = employeeDao.byLastNameLike("S").collect()

    smithsOrDoes should equal(Seq(
      Employee("222364883", "Carol", "Smith", 37),
      Employee("326587417", "Joe", "Stevens", 37),
      Employee("631231482", "David", "Smith", 77),
      Employee("845657246", "Kumar", "Swamy", 14)
    ))
  }

  "The sum of all the departments' budgets" should "be calculated" in {
    val budget = departmentDao.sumBudgets()

    budget should equal(375000)
  }

  "The number of all the employees in each department " should "be calculated" in {
    val numberOfEmployees = departmentDao.numberOfEmployees().collect()

    numberOfEmployees should equal(Seq(
      (37, 2),
      (59, 3),
      (77, 2),
      (14, 5)
    ))
  }

  "All employees including each employee's department's data" should "be selected" in {
    val employeesWithDepartment = employeeDao.withDepartment().collect()

    employeesWithDepartment should equal(Seq(
      ("222364883", "Carol", "Smith", "Accounting", 15000),
      ("326587417", "Joe", "Stevens", "Accounting", 15000),
      ("546523478", "John", "Doe", "Human Resources", 240000),
      ("654873219", "Zacary", "Efron", "Human Resources", 240000),
      ("745685214", "Eric", "Goldsmith", "Human Resources", 240000),
      ("332569843", "George", "ODonnell", "Research", 55000),
      ("631231482", "David", "Smith", "Research", 55000),
      ("123234877", "Michael", "Rogers", "IT", 65000),
      ("152934485", "Anand", "Manikutty", "IT", 65000),
      ("332154719", "Mary-Anne", "Foster", "IT", 65000),
      ("845657245", "Elizabeth", "Doe", "IT", 65000),
      ("845657246", "Kumar", "Swamy", "IT", 65000)
    ))
  }

}
