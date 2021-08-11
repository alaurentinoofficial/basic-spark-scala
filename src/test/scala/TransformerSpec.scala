import org.apache.spark.sql.types.{BooleanType, DoubleType, StringType, IntegerType, StructType, StructField}
import org.apache.spark.sql.functions.{lit}
import org.apache.spark.sql.{Row}

class TransformationSpec extends SparkSpec {

    import spark.implicits._

    feature("Transformer") {
        scenario("Acceptance test") {
            val schema = StructType(Array(
                StructField("Named", StringType, true),
                StructField("Age", IntegerType, true),
                StructField("Voted", BooleanType, true)
            ))
            
            val data = Seq(
                Row("A", 20, true),
                Row("B", 34, false),
                Row("C", 17, true),
                Row("D", 67, false),
                Row(null, 65, true),
                Row("E", 18, null)
            )

            val vote_df = spark.createDataFrame(
                spark.sparkContext.parallelize(data),
                StructType(schema)
            )

            val voted_df = Transformer(vote_df)

            val check_df = Seq(
                (true, 18.5),
                (false, 50.5)
            ).toDF("Voted", "Age-Avg")

            val check_schema = StructType(Array(
                StructField("Voted", BooleanType, true),
                StructField("Age-Avg", DoubleType, true)
            ))

            assert(voted_df.schema === check_schema)
            assert(voted_df.sort().collect === check_df.sort().collect)
        }
    }
}