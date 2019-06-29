import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer
case class ApacheLogEvent(ip: String,
                          userId: String,
                          eventTime: Long,
                          method: String,
                          url: String)

case class UrlViewCount(url: String,
                        windowEnd: Long,
                        count: Long)

object TrafficAnalysis {
  def main(args: Array[String]): Unit = {
    val resourcePath = getClass.getResource("/apachetest.log")

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(1)

    val stream = env
      .readTextFile(resourcePath.getPath)
      .map(line => {
        val linearray = line.split(" ")
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp = simpleDateFormat.parse(linearray(3)).getTime

        ApacheLogEvent(
          linearray(0),
          linearray(2),
          timestamp,
          linearray(5),
          linearray(6)
        )
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.milliseconds(1000)) {
        override def extractTimestamp(t: ApacheLogEvent): Long = {
          t.eventTime
        }
      })
      .keyBy("url")
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .aggregate(new CountAgg, new WindowResultFunction)
      .keyBy("windowEnd")
      .process(new TopNHotUrls(5))
      .print()

    env.execute
  }
}

class CountAgg extends AggregateFunction[ApacheLogEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class WindowResultFunction extends WindowFunction[Long,
  UrlViewCount, Tuple, TimeWindow] {
  override def apply(key: Tuple,
                     window: TimeWindow,
                     aggregateResult: Iterable[Long],
                     collector: Collector[UrlViewCount]): Unit = {
    val url: String = key.asInstanceOf[Tuple1[String]].f0
    val count = aggregateResult.iterator.next
    collector.collect(UrlViewCount(url, window.getEnd, count))
  }
}

class TopNHotUrls(topsize: Int) extends KeyedProcessFunction[Tuple, UrlViewCount, String] {
  private var urlState: ListState[UrlViewCount] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val urlSateDesc = new ListStateDescriptor[UrlViewCount]("urlState-state", classOf[UrlViewCount])
    urlState = getRuntimeContext.getListState(urlSateDesc)
  }

  override def processElement(input: UrlViewCount,
                              context: KeyedProcessFunction[Tuple, UrlViewCount, String]#Context,
                              collector: Collector[String]): Unit = {
    urlState.add(input)

    context.timerService.registerEventTimeTimer((input.windowEnd + 1))
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Tuple,
                         UrlViewCount,
                         String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    val allUrlViews: ListBuffer[UrlViewCount] = ListBuffer()
    import scala.collection.JavaConversions._
    for (urlView <- urlState.get) {
      allUrlViews += urlView
    }
    urlState.clear()
    val sortedUrlViews = allUrlViews.sortBy(_.count)(Ordering.Long.reverse)
      .take(topsize)

    var result: StringBuilder = new StringBuilder
    result.append("====================================\n")
    result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")

    for (i <- sortedUrlViews.indices) {
      val currentUrlView: UrlViewCount = sortedUrlViews(i)
      // e.g.  No1：  URL=/blog/tags/firefox?flav=rss20  流量=55
      result.append("No").append(i+1).append(":")
        .append("  URL=").append(currentUrlView.url)
        .append("  流量=").append(currentUrlView.count).append("\n")
    }
    result.append("====================================\n\n")
    // 控制输出频率，模拟实时滚动结果
    Thread.sleep(1000)
    out.collect(result.toString)
  }
}