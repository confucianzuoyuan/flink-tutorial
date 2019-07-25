import java.text.SimpleDateFormat
import java.util.{Date, UUID}
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessAllWindowFunction, ProcessWindowFunction}
import org.apache.flink.util.Collector

import scala.util.Random
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

//class SimulatedEventSource extends RichParallelSourceFunction[(String, String)] {
//
//  var running = true
//  val channelSet: Seq[String] = Seq("a", "b", "c", "d")
//  val behaviorTypes: Seq[String] = Seq(
//    "INSTALL", "OPEN", "BROWSE", "CLICK",
//    "PURCHASE", "CLOSE", "UNINSTALL")
//  val rand: Random = Random
//
//  override def run(ctx: SourceContext[(String, String)]): Unit = {
//    val numElements = Long.MaxValue
//    var count = 0L
//
//    while (running && count < numElements) {
//      val channel = channelSet(rand.nextInt(channelSet.size))
//      val event = generateEvent()
//      val ts = event.head.toLong
//      ctx.collectWithTimestamp((channel, event.mkString("\t")), ts)
//      count += 1
//      TimeUnit.MILLISECONDS.sleep(5L)
//    }
//  }
//
//  private def generateEvent(): Seq[String] = {
//    val dt = readableDate()
//    val id = UUID.randomUUID().toString
//    val behaviorType = behaviorTypes(rand.nextInt(behaviorTypes.size))
//    // (ts, readableDT, id, behaviorType)
//    Seq(dt._1.toString, dt._2, id, behaviorType)
//  }
//
//  private def readableDate(): (Long, String) = {
//    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    val ts = System.nanoTime
//    val dt = new Date(ts)
//    (ts, df.format(dt))
//  }
//
//  override def cancel(): Unit = running = false
//}

object ChannelStatisticNonKey {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream: DataStream[(String, String)] = env.addSource(new SimulatedEventSource)

    stream
      .map(t => {
        val channel = t._1
        val eventFields = t._2.split("\t")
        val ts = eventFields(0).toLong
        val behaviorType = eventFields(3)
        (ts, channel, behaviorType)
      })
      .assignTimestampsAndWatermarks(new TimestampExtractor(10))
      .map(t => (t._2, t._3))
      .timeWindowAll(Time.milliseconds(1000))
      .process(new MyReduceWindowAllFunction())
      .map(t => {
        val key = t._1
        val count = t._2
        val windowStartTime = key._1
        val windowEndTime = key._2
        val channel = key._3
        val behaviorType = key._4
        Seq(windowStartTime, windowEndTime, channel, behaviorType, count).mkString("\t")
      })
      .print()

    env.execute(getClass.getSimpleName)
  }
}

class TimestampExtractor(val maxLaggedTime: Long)
  extends AssignerWithPeriodicWatermarks[(Long, String, String)] with Serializable {

  var currentWatermarkTs = 0L

  override def getCurrentWatermark: Watermark = {
    if(currentWatermarkTs <= 0) {
      new Watermark(Long.MinValue)
    } else {
      new Watermark(currentWatermarkTs - maxLaggedTime)
    }
  }

  override def extractTimestamp(element: (Long, String, String),
                                previousElementTimestamp: Long): Long = {
    val ts = element._1
    Math.max(ts, currentWatermarkTs)
  }
}

class MyReduceWindowAllFunction
  extends ProcessAllWindowFunction[(String, String),
    ((String, String, String, String), Long), TimeWindow] {

  override def process(context: Context,
                       elements: Iterable[(String, String)],
                       collector: Collector[((String, String, String, String), Long)]): Unit = {
    val startTs = context.window.getStart
    val endTs = context.window.getEnd
    val elems = elements.map(t => {
      ((t._1, t._2), 1L)
    })
    for(group <- elems.groupBy(_._1)) {
      val myKey = group._1
      val myValue = group._2
      var count = 0L
      for(elem <- myValue) {
        count += elem._2
      }
      val channel = myKey._1
      val behaviorType = myKey._2
      val outputKey = (formatTs(startTs), formatTs(endTs), channel, behaviorType)
      collector.collect((outputKey, count))
    }
  }

  private def formatTs(ts: Long) = {
    val df = new SimpleDateFormat("yyyyMMddHHmmss")
    df.format(new Date(ts))
  }

}

