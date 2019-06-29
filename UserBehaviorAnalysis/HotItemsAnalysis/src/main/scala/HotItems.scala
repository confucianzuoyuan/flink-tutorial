import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

case class UserBehavior(userId: Long,
                        itemId: Long,
                        categoryId: Int,
                        behavior: String,
                        timestamp: Long)

case class ItemViewCount(itemId: Long,
                         windowEnd: Long,
                         count: Long)

object HotItems {
  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.  common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")



    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

      val stream = env.addSource(new FlinkKafkaConsumer[String]("hotitems0106", new SimpleStringSchema(), properties))

//    val stream = env
//      .readTextFile("/Users/yuanzuo/Desktop/UserBehaviorAnalysis/HotItemsAnalysis/src/main/resources/UserBehavior.csv")
        stream
      .map(line =>{
        val linearray = line.split(",")
        UserBehavior(linearray(0).toLong,
                     linearray(1).toLong,
                     linearray(2).toInt,
                     linearray(3),
                     linearray(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000)
      .filter(_.behavior == "pv")
      .keyBy("itemId")
      .timeWindow(Time.minutes(60), Time.minutes(5))
      .aggregate(new CountAgg(), new WindowResultFunction())
      .keyBy("windowEnd")
      .process(new TopItems(3)).print()

//    stream.print()

    env.execute
  }
}

class TopItems(topSize: Int) extends KeyedProcessFunction[Tuple,
  ItemViewCount, String] {
  private var itemState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    val itemsStateDesc = new ListStateDescriptor[ItemViewCount]("itemState-state",
      classOf[ItemViewCount])

    itemState = getRuntimeContext.getListState(itemsStateDesc)
  }

  override def processElement(input: ItemViewCount,
                              context: KeyedProcessFunction[Tuple, ItemViewCount,String]#Context,
                              collector: Collector[String]): Unit = {
    itemState.add(input)

    context.timerService.registerEventTimeTimer(input.windowEnd + 1)
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    val allItems: ListBuffer[ItemViewCount] = ListBuffer()
    import scala.collection.JavaConversions._
    for (item <- itemState.get) {
      allItems += item
    }
    itemState.clear()
    val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    val result: StringBuilder = new StringBuilder
    result.append("==================================")
    result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")

    for (i <- sortedItems.indices) {
      val currentItem: ItemViewCount = sortedItems(i)
      result
        .append("No")
        .append(i+1)
        .append(currentItem.itemId)
        .append("  浏览量=")
        .append(currentItem.count)
        .append("\n")
    }

    result.append("==================================")
    Thread.sleep(1000)
    out.collect(result.toString)
  }

}

class CountAgg extends AggregateFunction[UserBehavior, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class WindowResultFunction extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
  override def apply(key: Tuple,
                     window: TimeWindow,
                     aggregateResult: Iterable[Long],
                     collector: Collector[ItemViewCount]): Unit = {
    val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0
    val count = aggregateResult.iterator.next
    collector.collect(ItemViewCount(itemId, window.getEnd, count))
  }
}