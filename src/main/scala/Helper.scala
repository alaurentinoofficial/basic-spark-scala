import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions.{mean}
import javax.xml.crypto.Data

object Helper {
    def groupKeyAvgNumber(keyColumn: String, valueColumn: String)(df: DataFrame): DataFrame = {
        df.groupBy(keyColumn)
          .agg(mean(valueColumn).alias(s"$valueColumn-Avg"))
    }
}