package com.example.transforms

import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions.{mean}

object Transformer {
    def apply()(df: DataFrame): DataFrame = {
        df
        .transform(DropInvalidRows())
        .transform(GroupKeyAvgNumber("Voted", "Age"))
    }
}