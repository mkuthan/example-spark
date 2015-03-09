package example

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

/**
 * Notice: Variable binding is vulnerable for SQL injection.
 */
class EmployeeDao(sqlc: SQLContext) {

  import example.EmployeeDao._

  def lastNames(): RDD[String] = sqlc.sql("SELECT lastName FROM employees").map(row => row.getString(0))

  def distinctLastNames(): RDD[String] = sqlc.sql("SELECT DISTINCT lastName FROM employees").map(row => row.getString(0))

  def byLastName(lastNames: String*): RDD[Employee] = sqlc.sql(s"SELECT * FROM employees WHERE lastName IN(${lastNames.mkString("'", "', '", "'")})").map(toEmployee(_))

  def byLastNameLike(lastName: String): RDD[Employee] = sqlc.sql(s"SELECT * FROM employees WHERE lastName LIKE '${lastName}%'").map(toEmployee(_))

  def withDepartment(): RDD[(String, String, String, String, Int, Int)] = {
    val sql =
      """
        |SELECT ssn, e.name AS name_e, lastName, d.name AS name_d, budget
        | FROM employees e INNER JOIN departments d
        | ON e.department = d.code;
      """.stripMargin

    sqlc.sql(sql).map(row => (row.getString(0), row.getString(1), row.getString(2), row.getString(3), row.getInt(4), row.getInt(5)))
  }
}

object EmployeeDao {
  private def toEmployee(row: Row): Employee = Employee(row.getString(0), row.getString(1), row.getString(2), row.getInt(3))
}
