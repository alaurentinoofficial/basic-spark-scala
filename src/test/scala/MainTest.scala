import org.apache.spark.sql.types.{BooleanType, DoubleType, StructType, StructField}

class SampleSparkTest extends SparkTest {

    import spark.implicits._

    feature("Integration test") {
        scenario("should test group by voted and avg mean") {
            val vote_df = Seq(
                ("A", 20, true),
                ("B", 34, false),
                ("C", 17, true),
                ("D", 67, false)
            ).toDF("Name", "Age", "Voted")

            val check_df = Seq(
                (true, 18.5),
                (false, 50.5)
            ).toDF("Voted", "Age-Avg")

            val check_schema = StructType(Array(
                StructField("Voted", BooleanType, false),
                StructField("Age-Avg", DoubleType, true)
            ))

            Main.transform(vote_df).schema should be(check_schema)
            Main.transform(vote_df).sort().collect should be(check_df.sort().collect)
        }
    }
}