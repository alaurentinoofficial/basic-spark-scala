package com.example.transforms

import org.apache.spark.sql.types.{BooleanType, DoubleType, StringType, StructType, StructField}
import org.apache.spark.sql.functions.{lit}
import org.apache.spark.sql.{Row}
import com.example.{SparkSpec}
import com.example.transforms.{Gr}

class GroupKeyAvgNumberSpec extends SparkSpec {

    import spark.implicits._

    feature("GroupKeyAvgNumber") {
        scenario("should return the dataframe grouped by key and aggregated using mean") {
            val vote_df = Seq(
                ("A", 20, true),
                ("B", 34, false),
                ("C", 17, true),
                ("D", 67, false)
            ).toDF("Name", "Age", "Voted")

            val voted_df = vote_df.transform(GroupKeyAvgNumber("Voted", "Age"))

            val check_df = Seq(
                (true, 18.5),
                (false, 50.5)
            ).toDF("Voted", "Age-Avg")

            val check_schema = StructType(Array(
                StructField("Voted", BooleanType, false),
                StructField("Age-Avg", DoubleType, true)
            ))

            assert(voted_df.schema === check_schema)
            assert(voted_df.sort().collect === check_df.sort().collect)
        }
    }
}