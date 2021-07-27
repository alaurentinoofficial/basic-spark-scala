import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions.{mean}

object Helper {
    def groupKeyAvgNumber(keyColumn: String, valueColumn: String)(df: DataFrame) = {
        df.groupBy(keyColumn)
          .agg(mean(valueColumn).alias(s"$valueColumn-Avg"))
    }
}