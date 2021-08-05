import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.types._

object Main extends App {
    val spark = SparkSession
        .builder()
        .appName("Spark SQL basic example")
        .master("local")
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    import spark.implicits._
    
    // Create a sequence and convert to Dataframe using spark implicits
    val vote_df = Seq(
        ("A", 20, true),
        ("B", 34, false),
        ("C", 17, true),
        ("D", 67, false)
    ).toDF("Name", "Age", "Voted")
    
    // Show the dataframe
    vote_df.show()
    
    // Explain the average transformation
    Helper.transform(vote_df)
        .explain()
    
    // Show the average transformation
    Helper.transform(vote_df)
        .show()
}