
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
class Windowing {


  def dateDiff(df : DataFrame): DataFrame = {
    val w = Window.orderBy("device_timestamp")
    val diff = col("device_timestamp").cast("long")-lag("device_timestamp", 1).over(w).cast("long")

    df.withColumn("dif",diff)
  }

}
