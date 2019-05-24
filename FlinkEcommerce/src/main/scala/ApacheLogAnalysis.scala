import java.sql.Timestamp
import java.util.{ArrayList, Comparator, List, Properties}

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.tuple.Tuple1
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.api.common.state.ListState
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.configuration.Configuration
import org.joda.time.format.DateTimeFormat

object ApacheLogAnalysis {

  class ApacheLogCountAgg extends AggregateFunction[ApacheLogEvent, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(apacheLogEvent: ApacheLogEvent, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc1: Long, acc2: Long): Long = acc1 + acc2
  }

  def main(args: Array[String]): Unit = {
    val resourcesPath = getClass.getResource("/apachetest.log")
    println(resourcesPath.getPath)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val stream = env
      .readTextFile(resourcesPath.getPath)
      .map(line => {
        val linearray = line.split(" ")
        val dtf = DateTimeFormat.forPattern("dd/MM/yyyy:HH:mm:ss")
        val timestamp = dtf.parseDateTime(linearray(3)).getMillis().toString()
        ApacheLogEvent(linearray(0), linearray(2), timestamp, linearray(5), linearray(6))
      })
      .assignAscendingTimestamps(_.eventTime.toLong)
      .keyBy(("ip"))
      .timeWindow(Time.seconds(60), Time.seconds(5))
      .aggregate(new ApacheLogCountAgg(), new ApacheLogWindowResultFunction())
      .keyBy(1)
      .process(new ApacheLogTopNHotItems(3))
      .print()

    env.execute("Hot Items Job")
  }

  class ApacheLogTopNHotItems extends KeyedProcessFunction[Tuple, IpViewCount, String] {
    private var topSize = 0

    def this(topSize: Int) {
      this()
      this.topSize = topSize
    }

    private var itemState : ListState[IpViewCount] = null

    @throws[Exception]
    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      // 命名状态变量的名字和状态变量的类型
      val itemsStateDesc = new ListStateDescriptor[IpViewCount]("itemState-state", classOf[IpViewCount])
      // 从运行时上下文中获取状态并赋值
      itemState = getRuntimeContext.getListState(itemsStateDesc)
    }

    @throws[Exception]
    override def processElement(input: IpViewCount, context: KeyedProcessFunction[Tuple, IpViewCount, String]#Context, collector: Collector[String]): Unit = { // 每条数据都保存到状态中
      itemState.add(input)
      // 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有商品数据
      // 也就是当程序看到windowend + 1的水位线watermark时，触发onTimer回调函数
      context.timerService.registerEventTimeTimer(input.windowEnd + 1)
    }

    @throws[Exception]
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, IpViewCount, String]#OnTimerContext, out: Collector[String]): Unit = { // 获取收到的所有商品点击量
      var allItems: List[IpViewCount] = new ArrayList[IpViewCount]
      import scala.collection.JavaConversions._
      for (item <- itemState.get) {
        allItems.add(item)
      }
      // 提前清除状态中的数据，释放空间
      itemState.clear()
      // 按照点击量从大到小排序
      allItems.sort(new Comparator[IpViewCount]() {
        override def compare(o1: IpViewCount, o2: IpViewCount): Int = (o2.count - o1.count).toInt
      })
      // 将排名信息格式化成 String, 便于打印
      var result: StringBuilder = new StringBuilder
      result.append("====================================\n")
      result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")
      var i: Int = 0
      while (i < allItems.size && i < topSize) {
        var currentItem: IpViewCount = allItems.get(i)
        // No1:  商品ID=12224  浏览量=2413
        result.append("No").append(i).append(":").append("  IP=").append(currentItem.ip).append("  流量=").append(currentItem.count).append("\n")
        i += 1
      }
      result.append("====================================\n\n")
      // 控制输出频率，模拟实时滚动结果
      Thread.sleep(1000)
      out.collect(result.toString)
    }

  }

  class ApacheLogWindowResultFunction extends WindowFunction[Long, IpViewCount, Tuple, TimeWindow] {
    override def apply(key: Tuple, window: TimeWindow, aggregateResult: Iterable[Long], collector: Collector[IpViewCount]) : Unit = {
      val ip: String = key.asInstanceOf[Tuple1[String]].f0
      val count = aggregateResult.iterator.next
      collector.collect(IpViewCount(ip, window.getEnd, count))
    }
  }
}