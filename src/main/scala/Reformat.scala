import org.apache.spark.sql.SparkSession

class Reformat {
  import org.apache.spark.sql.types._
  import org.apache.spark.sql.functions._

  val schm = StructType(Seq())
  val parquetLoc = ""
  val spark = SparkSession
    .builder()
    .appName("FlatDataFrame")
    .enableHiveSupport()
    .getOrCreate()

  def reformatAndWrite(pathToFile : String): Unit = {
    val df = spark.read.format("com.databricks.spark.csv").schema(schm).option("header","true").option("delimiter","\t").option("nullValue","NULL").option("treatEmptyValuesAsNulls","true").load(pathToFile)
    // get file name and remove txt
    val s = pathToFile.split("/").last
    val fileName = s.substring(0,s.length-3)
    println("Writing: "+parquetLoc+fileName+"parquet")
    df.write.parquet(parquetLoc+fileName+"parquet")
  }
}
