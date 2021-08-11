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
        ("D", 67, false),
        ("E", 65, true)
    ).toDF("Name", "Age", "Voted")
    
    // Show the dataframe
    vote_df.show()
    
    // Explain the average transformation
    Transformer(vote_df)
        .explain()
    
    // Show the average transformation
    Transformer(vote_df)
        .show()

    val schema = StructType(Array(
        StructField("Named", StringType, true),
        StructField("Age", DoubleType, true),
        StructField("Voted", BooleanType, true)
    ))

    val data = Seq(
        Row("A", 20.0, true),
        Row("B", 34.0, false),
        Row("Z", null, null),
        Row("C", 17.0, true),
        Row(null, 68.0, null),
        Row("D", 67.0, false),
        Row(null, null, null)
    )

    val vote2_df = spark.createDataFrame(
        spark.sparkContext.parallelize(data),
        schema
    )

    val exec_df = vote2_df.transform(DropInvalidRows())

    exec_df.show()
}