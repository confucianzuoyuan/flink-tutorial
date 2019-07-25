import java.text.SimpleDateFormat
import java.util.{Date, UUID}
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.util.Collector
import scala.util.Random
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class SimulatedEventSource extends RichParallelSourceFunction[(String, String)] {

  var running = true
  val channelSet: Seq[String] = Seq("a", "b", "c", "d")
  val behaviorTypes: Seq[String] = Seq(
    "INSTALL", "OPEN", "BROWSE", "CLICK",
    "PURCHASE", "CLOSE", "UNINSTALL")
  val rand: Random = Random

  override def run(ctx: SourceContext[(String, String)]): Unit = {
    val numElements = Long.MaxValue
    var count = 0L

    while (running && count < numElements) {
      val channel = channelSet(rand.nextInt(channelSet.size))
      val event = generateEvent()
      val ts = event.head.toLong
      ctx.collectWithTimestamp((channel, event.mkString("\t")), ts)
      count += 1
      TimeUnit.MILLISECONDS.sleep(5L)
    }
  }

  private def generateEvent(): Seq[String] = {
    val dt = readableDate
    val id = UUID.randomUUID().toString
    val behaviorType = behaviorTypes(rand.nextInt(behaviorTypes.size))
    // (ts, readableDT, id, behaviorType)
    Seq(dt._1.toString, dt._2, id, behaviorType)
  }

  private def readableDate = {
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val ts = System.nanoTime
    val dt = new Date(ts)
    (ts, df.format(dt))
  }

  override def cancel(): Unit = running = false
}

object ChannelStatistic {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setParallelism(1)
    val stream: DataStream[(String, String)] = env.addSource(new SimulatedEventSource)

    stream
      .map(t => {
        val channel = t._1
        val eventFields = t._2.split("\t")
        val behaviorType = eventFields(3)
        ((channel, behaviorType), 1L)
      })
      .keyBy(0)
      .timeWindow(Time.seconds(5), Time.seconds(1))
      .process(new MyReduceWindowFunction)
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

class MyReduceWindowFunction
  extends ProcessWindowFunction[((String, String), Long), ((String, String, String, String), Long), Tuple, TimeWindow] {

  override def process(key: Tuple,
                       context: Context,
                       elements: Iterable[((String, String), Long)],
                       collector: Collector[((String, String, String, String), Long)]): Unit = {

    val startTs = context.window.getStart
    val endTs = context.window.getEnd

    for(group <- elements.groupBy(_._1)) {
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