package com.example.transforms

import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions.{mean}

object GroupKeyAvgNumber {
    def apply(keyColumn: String, valueColumn: String)(df: DataFrame): DataFrame = {
        df
        .groupBy(keyColumn)
        .agg(mean(valueColumn).alias(s"$valueColumn-Avg"))
    }
}