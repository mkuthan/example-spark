package example

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * http://en.wikibooks.org/wiki/SQL_Exercises/Employee_management
 */
object SparkSqlExample extends LazyLogging {

  private val master = "local[2]"
  private val appName = "example-spark"

  def main(args: Array[String]) = {

    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    val sc = new SparkContext(conf)
    val sqlc = new SQLContext(sc)

    val employeeDao = new EmployeeDao(sqlc)
    val departmentDao = new DepartmentDao(sqlc)

    import sqlc.implicits._

    val employees = sc.textFile("src/main/resources/data/employees.txt")
      .map(_.split(","))
      .map(fields => Employee(fields(0), fields(1), fields(2), fields(3).trim.toInt))
    employees.toDF().registerTempTable("employees")

    val departments = sc.textFile("src/main/resources/data/departments.txt")
      .map(_.split(","))
      .map(fields => Department(fields(0).trim.toInt, fields(1), fields(2).trim.toInt))
    departments.toDF().registerTempTable("departments")

    logger.info("Select the last name of all employees")
    employeeDao.lastNames().collect().foreach(println)

    logger.info("Select the last name of all employees, without duplicates.")
    employeeDao.distinctLastNames().collect().foreach(println)

    logger.info("Select all the data of employees whose last name is \"Smith\".")
    employeeDao.byLastName("Smith").collect().foreach(println)

    logger.info("Select all the data of employees whose last name is \"Smith\" or \"Doe\".")
    employeeDao.byLastName("Smith", "Doe").collect().foreach(println)

    logger.info("Select all the data of employees whose last name begins with an \"S\".")
    employeeDao.byLastNameLike("S").collect().foreach(println)

    logger.info("Select the sum of all the departments' budgets.")
    println(departmentDao.sumBudgets())

    logger.info("Select the number of employees in each department.")
    departmentDao.numberOfEmployees().collect().foreach(println)

    //logger.info("Select all the data of employees, including each employee's department's data.")
    //val employeesWithDepartments = employeeDao.withDepartment()
    //employeesWithDepartments.collect().foreach(println)

  }

}
