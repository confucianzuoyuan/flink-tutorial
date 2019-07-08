import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


object test {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    val inputStream: DataStream[(String, List[String])] = env.fromElements(
//      ("en", List("tea")), ("fr", List("vin")), ("en", List("cake")))
//
//    val resultStream: DataStream[(String, List[String])] = inputStream
//      .keyBy(0)
//      .reduce((x, y) => (x._1, x._2 ::: y._2))
//
//    resultStream.print()

    val sensorData = env.socketTextStream("localhost", 9999)

    val minTempPerWindow: DataStream[(String, Double, Long)] = sensorData
      .map(line => {
        val r = line.split("\\s")
        (r(0), r(1).toDouble, r(2).toLong)
      })
//      .assignAscendingTimestamps(_._3)
      .assignTimestampsAndWatermarks(
        new SensorTimeAssigner
      )
      // 按照传感器id分流
      .keyBy(_._1)
      .timeWindow(Time.seconds(15), Time.seconds(5))
      .reduce((r1, r2) => (r1._1, r1._2.min(r2._2), r1._3))

    minTempPerWindow.map(r => (r._1, r._2)).print()

    env.execute
  }

  class SensorTimeAssigner
    extends BoundedOutOfOrdernessTimestampExtractor[(String, Double, Long)](Time.seconds(1)) {

    // 抽取时间戳
    override def extractTimestamp(r: (String, Double, Long)): Long = r._3 * 1000
  }
}