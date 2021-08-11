package com.example.transforms

import org.apache.spark.sql.types.{BooleanType, DoubleType, StringType, IntegerType, StructType, StructField}
import org.apache.spark.sql.functions.{lit}
import org.apache.spark.sql.{Row}
import com.example.{SparkSpec}

class DropInvalidRowsSpec extends SparkSpec {

    import spark.implicits._

    feature("DropInvalidRows") {
        scenario("should return the dataframe without nulls in selected columns") {
            val schema = StructType(Array(
                StructField("Named", StringType, true),
                StructField("Age", IntegerType, true),
                StructField("Voted", BooleanType, true)
            ))

            val data = Seq(
                Row("A", 20, true),
                Row("B", 34, false),
                Row("Z", null, null),
                Row("C", 17, true),
                Row(null, 68, null),
                Row("D", 67, false),
                Row(null, null, null)
            )

            val vote_df = spark.createDataFrame(
                spark.sparkContext.parallelize(data),
                schema
            )

            val exec_df = vote_df.transform(DropInvalidRows())

            val check_df = Seq(
                ("A", 20, true),
                ("B", 34, false),
                ("C", 17, true),
                ("D", 67, false)
            ).toDF("Name", "Age", "Voted")

            assert(exec_df.sort().collect === check_df.sort().collect)
        }
    }
}