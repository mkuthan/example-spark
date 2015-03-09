package example

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

/**
 * Notice: Variable binding is vulnerable for SQL injection.
 */
class DepartmentDao(sqlc: SQLContext) {

  def sumBudgets(): Long = sqlc.sql("SELECT SUM(budget) FROM departments").map(row => row.getLong(0)).first()

  def numberOfEmployees(): RDD[(Int, Long)] = sqlc.sql("SELECT department, COUNT(*) FROM employees GROUP BY department").map(row => (row.getInt(0), row.getLong(1)))

}
