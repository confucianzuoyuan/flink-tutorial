import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import org.joda.time.DateTime
import org.apache.flink.streaming.api.scala._
import java.util.Locale

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.tuple.Tuple1
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.util.Collector
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

/**
  *
  * The task of the exercise is to identify every five minutes popular areas where many taxi rides
  * arrived or departed in the last 15 minutes.
  *
  * 滑动窗口：窗口的大小：15分钟；滑窗的步长：5分钟
  * 写入到es中
  *
  * The results are written into an Elasticsearch index.
  *
  * Parameters:
  * -input path-to-input-file
  *
  * ES和Kibana的版本为6.5.1
  *
  * kibana默认监听端口：5601
  *
  * 在命令行运行以下两条命令, 创建索引和mapping，告诉es数据的类型是什么
  *
  * curl -XPUT "http://localhost:9200/nyc-idx"
  *
  * curl -XPUT -H'Content-Type: application/json' "http://localhost:9200/nyc-idx/_mapping/popular-locations" -d'
  * {
  *  "popular-locations" : {
  *    "properties" : {
  *       "cnt": {"type": "integer"},
  *       "location": {"type": "geo_point"},
  *       "time": {"type": "date"}
  *     }
  *  }
  * }'
  *
  */

case class PvLocation(timestamp: Long, Longitude: Float, Latitude: Float)

//<cellId, windowTime, isStart, cnt>
case class PvAgg(cellId: Long, windowTime: Long, cnt: Long)

object PvLocationToES {
  // geo boundaries of the area of NYC
  var LON_EAST: Double = -73.7
  var LON_WEST: Double = -74.05
  var LAT_NORTH = 41.0
  var LAT_SOUTH = 40.5
  // delta step to create artificial grid overlay of NYC
  var DELTA_LON = 0.0014
  var DELTA_LAT = 0.00125

  // ( |LON_WEST| - |LON_EAST| ) / DELTA_LAT
  var NUMBER_OF_GRID_X = 250
  val timeFormatter: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withLocale(Locale.US).withZoneUTC

  def main(args: Array[String]): Unit = {
    val resourcesPath = getClass.getResource("/pvlocation.csv")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env
      .readTextFile(resourcesPath.getPath)
      .map(line => {
        val linearray = line.split(",")
        val ts: Long = DateTime.parse(linearray(0), timeFormatter).getMillis
        PvLocation(ts, linearray(1).toFloat, linearray(2).toFloat)
      })
      .assignTimestampsAndWatermarks(
        new LogTimeAssigner
      )
      .filter(l => isInNYC(l.Longitude, l.Latitude))
      // 将每一个行程映射到一个单元格里面，方便统计
      .map(l => mapToGridCell(l.Longitude, l.Latitude))
      .keyBy(0)
      .timeWindow(Time.minutes(15), Time.minutes(5))
      .apply(new PvCounter())
      .map(r => (
        getGridCellCenterLon(r.cellId.toInt),
        getGridCellCenterLat(r.cellId.toInt),
        r.windowTime,
        r.cnt
      ))

    val httpHosts = new java.util.ArrayList[HttpHost]
    httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"))

    val esSinkBuilder = new ElasticsearchSink.Builder[(Float, Float, Long, Long)](
      httpHosts,
      new ElasticsearchSinkFunction[(Float, Float, Long, Long)] {
        def createIndexRequest(record: (Float, Float, Long, Long)): IndexRequest = {
          val json = new java.util.HashMap[String, String]
          json.put("time", record._3.toString)         // timestamp, windowend time
          json.put("location", record._2 + "," + record._1)  // lat,lon pair
          json.put("cnt", record._4.toString)

          Requests.indexRequest()
            .index("nyc-idx")
            .`type`("popular-locations")
            .source(json)
        }

        override def process(record: (Float, Float, Long, Long), ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
          indexer.add(createIndexRequest(record))
        }
      }
    )

    stream.addSink(esSinkBuilder.build)

    env.execute
  }

  def isInNYC(lon: Float, lat: Float): Boolean = !(lon > LON_EAST || lon < LON_WEST) && !(lat > LAT_NORTH || lat < LAT_SOUTH)

  def mapToGridCell(lon: Float, lat: Float): Int = {
    val xIndex = Math.floor((Math.abs(LON_WEST) - Math.abs(lon)) / DELTA_LON).toInt
    val yIndex = Math.floor((LAT_NORTH - lat) / DELTA_LAT).toInt
    xIndex + (yIndex * NUMBER_OF_GRID_X)
  }

  def getGridCellCenterLon(gridCellId: Int): Float = {
    val xIndex = gridCellId % NUMBER_OF_GRID_X
    (Math.abs(LON_WEST) - (xIndex * DELTA_LON) - (DELTA_LON / 2)).asInstanceOf[Float] * -1.0f
  }

  def getGridCellCenterLat(gridCellId: Int): Float = {
    val xIndex = gridCellId % NUMBER_OF_GRID_X
    val yIndex = (gridCellId - xIndex) / NUMBER_OF_GRID_X
    (LAT_NORTH - (yIndex * DELTA_LAT) - (DELTA_LAT / 2)).asInstanceOf[Float]
  }

  class LogTimeAssigner
    extends BoundedOutOfOrdernessTimestampExtractor[PvLocation](Time.seconds(10)) {

    // 抽取时间戳
    override def extractTimestamp(r: PvLocation): Long = r.timestamp
  }

  class PvCounter extends WindowFunction[Int, PvAgg, Tuple, TimeWindow] {
    override def apply(key: Tuple, window: TimeWindow, aggregateResult: Iterable[Int], out: Collector[PvAgg]) : Unit = {
      val cellId = key.asInstanceOf[Tuple1].f0
      val windowTime = window.getEnd

      var cnt = 0
      // 单元格在窗口中出现的次数
      for (c <- aggregateResult) {
        cnt += 1
      }

      out.collect(PvAgg(cellId.toString.toLong, windowTime, cnt))
    }
  }
}