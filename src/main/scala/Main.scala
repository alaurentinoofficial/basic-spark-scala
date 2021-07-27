import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row}
import org.apache.spark.sql.types._

object Main extends App {
    val spark = SparkSession
        .builder()
        .appName("Spark SQL basic example")
        .master("local")
        .config("spark.executor.instances", "4")
        .config("spark.executor.memory", "2g")
        .getOrCreate()
    
    import spark.implicits._
    
    val voteRDD = spark.sparkContext.parallelize(List(
        Row("Anderson", 20, true),
        Row("Fulano", 34, false),
        Row("Ciclano", 17, true),
        Row("Deltrano", 67, false)
    ))

    val voteSchema = StructType(List(
        StructField("Name", StringType, true),
        StructField("Age", IntegerType, true),
        StructField("Voted", BooleanType, true)
    ))

    val vote_df = spark.createDataFrame(voteRDD, voteSchema)

    vote_df.show()

    vote_df
        .filter($"Voted" && $"Name" =!= "Anderson")
        .show()

    vote_df
        .groupBy($"Voted")
        .agg(sum($"Age").alias("AgeAvg"))
        .explain()
}