package com.example.transforms

import org.apache.spark.sql.{DataFrame}

object DropInvalidRows {
    def apply()(df: DataFrame): DataFrame = {
        df.na.drop()
    }
}