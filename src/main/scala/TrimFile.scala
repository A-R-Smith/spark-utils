import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types._
//import org.apache.spark.sql.functions.desc

class TrimFile {


  val schm = StructType(Seq(StructField("imei",LongType,true), StructField("device_timestamp",DateType,true), StructField("alarm_status",StringType,true), StructField("record_hash",StringType,true), StructField("motive_mode",StringType,true), StructField("fuel_level",DoubleType,true), StructField("crash_event_type",StringType,true), StructField("latitude",DoubleType,true), StructField("longitude",DoubleType,true), StructField("gps_direction",DoubleType,true), StructField("gps_pdop",DoubleType,true), StructField("odometer_value_in_km",DoubleType,true), StructField("odometer_value_in_miles",DoubleType,true), StructField("odometer_error",DoubleType,true), StructField("gps_speed_in_milliknots",DoubleType,true), StructField("gps_speed_in_kph",DoubleType,true), StructField("acceleration_latitudinal",DoubleType,true), StructField("acceleration_longitudinal",DoubleType,true), StructField("driver_seatbelt_status",StringType,true), StructField("passenger_seatbelt_status",StringType,true), StructField("second_row_left_seatbelt_status",StringType,true), StructField("second_row_middle_seatbelt_status",StringType,true), StructField("second_row_right_seatbelt_status",StringType,true), StructField("third_row_left_seatbelt_status",StringType,true), StructField("third_row_middle_seatbelt_status",StringType,true), StructField("third_row_right_seatbelt_status",StringType,true), StructField("driver_door_status",StringType,true), StructField("passenger_door_status",StringType,true), StructField("rear_driver_door_status",StringType,true), StructField("rear_passenger_door_status",StringType,true), StructField("hood_status",StringType,true), StructField("tailgate_status",StringType,true), StructField("vehicle_lock_status",StringType,true), StructField("driver_air_bag_deploy",StringType,true), StructField("passenger_air_bag_deploy",StringType,true), StructField("passenger_seat_occupied",StringType,true), StructField("stability_control",StringType,true), StructField("vehicle_speed_in_kph",DoubleType,true), StructField("awd_usage",StringType,true), StructField("gear_lever_position",StringType,true), StructField("ignition_status",StringType,true), StructField("ecall_info_ecallconfirmation",StringType,true), StructField("edr_triggered",StringType,true), StructField("awd_fault_indicator",StringType,true), StructField("oil_level_low_indicator",StringType,true), StructField("engine_service_required_indicator",StringType,true), StructField("transmission_fault_indicator",StringType,true), StructField("abs_fault_indicator",StringType,true), StructField("headlight_status",StringType,true), StructField("oil_life",StringType,true), StructField("left_front_tire_pressure",DoubleType,true), StructField("lf_tp_error",DoubleType,true), StructField("right_front_tire_pressure",DoubleType,true), StructField("rf_tp_error",DoubleType,true), StructField("right_rear_tire_pressure",DoubleType,true), StructField("rr_tp_error",DoubleType,true), StructField("left_rear_tire_pressure",DoubleType,true), StructField("lr_tp_error",DoubleType,true), StructField("engine_coolant_temperature",DoubleType,true), StructField("transmission_temperature",DoubleType,true), StructField("transmission_temperatur_error",DoubleType,true), StructField("collision_mitigation_braking_event",StringType,true), StructField("forward_collision_event",StringType,true), StructField("cross_traffic_alert_left",StringType,true), StructField("cross_traffic_alert_right",StringType,true), StructField("blis_status_left",StringType,true), StructField("blis_status_right",StringType,true), StructField("abs_event",StringType,true), StructField("hands_off_wheel_event",StringType,true), StructField("forward_collision_disabled",StringType,true), StructField("forward_collision_sensitivity_change",StringType,true), StructField("das_warning",StringType,true), StructField("driver_alert_system",StringType,true), StructField("lane_departure_warning",StringType,true), StructField("lane_keep_alert",StringType,true), StructField("advanced_track_traction_control_active",StringType,true), StructField("trailer_sway_event",StringType,true), StructField("load_shed_event",StringType,true)))
  val imeiSchm = StructType(Seq(StructField("date",DateType,true), StructField("imei",LongType,true), StructField("monthly sum",IntegerType,true)))
  val imeiFile = ""
  val spark = SparkSession
    .builder()
    .appName("FlatDataFrame")
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._

  def processFiles(dir : String): Unit = {
    val imeiDf = spark.read.format("com.databricks.spark.csv")
      .option("header","true")
      .option("delimiter",",")
      .load(imeiFile)

    val fs = FileSystem.get(new Configuration())
    val list = fs.listStatus(new Path(dir))
    val dfs = list.map(fn=>trimFile(fn.getPath.toString,imeiDf))

  }


  def trimFile(fileName : String, imeiDf : DataFrame): DataFrame = {
    val df = spark.read.format("com.databricks.spark.csv")
                  .schema(schm)
                  .option("header","true")
                  .option("delimiter","\t")
                  .option("nullValue","NULL")
                  .option("treatEmptyValuesAsNulls","true")
                  .load(fileName)
    
    val dateReg = raw"\d{4}-\d{2}-\d{2}".r
    val date = dateReg.findFirstIn(String).getOrElse(None)
    val d = imeiDf.filter($"date" > date)
    imeiDf.sort("date")
    val d1 = df.cache()
//    df.groupBy("imei").count()
//    val k = df.groupBy($"imei").count().sort(desc("count"))
    df
  }
}
