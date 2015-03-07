package example

import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.scalatest._

class SparkSqlExampleSpec extends FlatSpec with BeforeAndAfter with GivenWhenThen with Matchers {

  private val master = "local[2]"
  private val appName = "example-spark"

  private var sc: SparkContext = _
  private var sqlc: SQLContext = _

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    sc = new SparkContext(conf)
    sqlc = new SQLContext(sc)

    val employees = sc.parallelize(Array(
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
    ))
    //employees.registerTempTable("employees")

    val departments = sc.parallelize(Array(
      Department(14, "IT", 65000),
      Department(37, "Accounting", 15000),
      Department(59, "Human Resources", 240000),
      Department(77, "Research", 55000)
    ))
    //departments.registerTempTable("departments")


  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }


}
