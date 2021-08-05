import org.apache.log4j.lf5.LogLevel
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


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

        Main.transform(vote_df).sort().collect should be(check_df.sort().collect)
        }
    }
}