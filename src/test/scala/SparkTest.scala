import org.apache.spark.sql.SparkSession
import org.scalatest.{FeatureSpec, GivenWhenThen, Matchers}

class SparkTest extends FeatureSpec with GivenWhenThen with Matchers {
  val spark: SparkSession = SparkSession.builder
    .appName("Spark SQL basic example")
    .config("spark.driver.host","127.0.0.1")
    .master("local")
    .getOrCreate()
}