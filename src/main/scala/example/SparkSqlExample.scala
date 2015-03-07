package example

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql._

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

    import sqlc.createSchemaRDD

    val employees = sc.textFile("src/main/resources/data/employees.txt")
      .map(_.split(","))
      .map(fields => Employee(fields(0), fields(1), fields(2), fields(3).trim.toInt))
    employees.registerTempTable("employees")

    val departments = sc.textFile("src/main/resources/data/departments.txt")
      .map(_.split(","))
      .map(fields => Department(fields(0).trim.toInt, fields(1), fields(2).trim.toInt))
    departments.registerTempTable("departments")

    logger.info("Select the last name of all employees")
    val lastNames = sqlc.sql("SELECT lastName FROM employees")
    lastNames.collect().foreach(println)

    logger.info("Select the last name of all employees, without duplicates.")
    val distinctLastNames = sqlc.sql("SELECT DISTINCT lastName FROM employees")
    distinctLastNames.collect().foreach(println)

    logger.info("Select all the data of employees whose last name is \"Smith\".")
    val smith = sqlc.sql("SELECT * FROM employees WHERE lastName = 'Smith'")
    smith.collect().foreach(println)

    logger.info("Select all the data of employees whose last name is \"Smith\" or \"Doe\".")
    val smithOrDoe = sqlc.sql("SELECT * FROM employees WHERE lastName IN ('Smith', 'Doe')")
    smithOrDoe.collect().foreach(println)

    logger.info("Select all the data of employees whose last name begins with an \"S\".")
    val beginsWithS = sqlc.sql("SELECT * FROM employees WHERE lastName LIKE 'S%'")
    beginsWithS.collect().foreach(println)

    logger.info("Select the sum of all the departments' budgets.")
    val budgets = sqlc.sql("SELECT SUM(budget) FROM departments")
    budgets.collect().foreach(println)

    logger.info("Select the number of employees in each department.")
    val numberOfEmployees = sqlc.sql("SELECT department, COUNT(*)  FROM employees GROUP BY department")
    numberOfEmployees.collect().foreach(println)

    logger.info("Select all the data of employees, including each employee's department's data.")
    val employyesWithDepartmentsSql =
      """
        |SELECT ssn, e.name AS name_e, lastName, d.name AS name_d, department, code, budget
        | FROM employees e INNER JOIN departments d
        | ON e.department = d.code;
      """.stripMargin
    //val employyesWithDepartments = sqlc.sql(employyesWithDepartmentsSql)
    //employyesWithDepartments.collect().foreach(println)

  }

}
