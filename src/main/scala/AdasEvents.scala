import org.apache.spark.sql.{DataFrame, SparkSession}
import java.util.concurrent.atomic._
class AdasEvents {
  case class Event(imei: Long, device_timestamp : Long, forward_collision_event: String)
  case class FullEvent(imei: Long, start : Long, stop : Long)
  def doIt(fwdcols : DataFrame): Unit = {
    case class Event(imei: Long, device_timestamp : java.sql.Timestamp, forward_collision_event: String)
    case class FullEvent(imei: Long, start : java.sql.Timestamp, stop : java.sql.Timestamp)
    var evnts = Array[FullEvent]()
    var lastEvnt : Event = null
    var i = new AtomicInteger(1)
    fwdcols.rdd.foreach(row=>{
      val currentEvnt=new Event(row.getLong(0), row.getTimestamp(1), row.getString(2))
      i.incrementAndGet();
      if (lastEvnt!=null) {
        if (lastEvnt.forward_collision_event=="on" && lastEvnt.imei==currentEvnt.imei) {
          if (currentEvnt.forward_collision_event=="off") {
            val full = new FullEvent(lastEvnt.imei,lastEvnt.device_timestamp,currentEvnt.device_timestamp)
            evnts :+ full

          }
        }

      }
      lastEvnt = currentEvnt
    })
//    evnts.
  }
  def doIt(spark : SparkSession): Unit = {
    val d = spark.sqlContext.createDataFrame(Seq(
      (1.0, "a"),
      (0.0, "b"),
      (0.0, "b"),
      (1.0, "a")
    )).toDF("label", "features")
    val x = d.collect()
    var i = 1
    x.fold(i){(row,x)=>

      i+=x._1
    }
  }
}
