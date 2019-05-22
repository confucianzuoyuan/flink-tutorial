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

object HotCats {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val stream = env
      .readTextFile("/Users/yuanzuo/Desktop/flink-tutorial/atguiguflink/src/main/resources/UserBehavior.csv")
      .map(line => {
        val linearray = line.split(",")
        (linearray(0).toLong, linearray(1).toLong, linearray(2).toLong, linearray(3).toString, linearray(4).toLong)
      })
      .assignAscendingTimestamps(_._5 * 1000)
//      .filter(_._4.equals("pv"))
      .keyBy(2)
      .timeWindow(Time.minutes(60), Time.minutes(5))
      .aggregate(new CountAgg(), new WindowResultFunction())
      .keyBy(1)
      .process(new TopNHotCats(3))
      .print()

    env.execute("Hot Cats Job")
  }

  class TopNHotCats extends KeyedProcessFunction[Tuple, (Long, Long, Long), String] {
    private var topSize = 0

    def this(topSize: Int) {
      this()
      this.topSize = topSize
    }

    private var catState : ListState[(Long, Long, Long)] = null

    @throws[Exception]
    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      // 命名状态变量的名字和状态变量的类型
      val itemsStateDesc = new ListStateDescriptor[(Long, Long, Long)]("itemState-state", classOf[(Long, Long, Long)])
      // 从运行时上下文中获取状态并赋值
      catState = getRuntimeContext.getListState(itemsStateDesc)
    }

    @throws[Exception]
    override def processElement(input: (Long, Long, Long), context: KeyedProcessFunction[Tuple, (Long, Long, Long), String]#Context, collector: Collector[String]): Unit = { // 每条数据都保存到状态中
      catState.add(input)
      // 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有商品数据
      // 也就是当程序看到windowend + 1的水位线watermark时，触发onTimer回调函数
      context.timerService.registerEventTimeTimer(input._2 + 1)
    }

    @throws[Exception]
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, (Long, Long, Long), String]#OnTimerContext, out: Collector[String]): Unit = { // 获取收到的所有商品点击量
      var allCats: List[(Long, Long, Long)] = new ArrayList[(Long, Long, Long)]
      import scala.collection.JavaConversions._
      for (item <- catState.get) {
        allCats.add(item)
      }
      // 提前清除状态中的数据，释放空间
      catState.clear()
      // 按照点击量从大到小排序
      allCats.sort(new Comparator[(Long, Long, Long)]() {
        override def compare(o1: (Long, Long, Long), o2: (Long, Long, Long)): Int = (o2._3 - o1._3).toInt
      })
      // 将排名信息格式化成 String, 便于打印
      var result: StringBuilder = new StringBuilder
      result.append("====================================\n")
      result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")
      var i: Int = 0
      while (i < allCats.size && i < topSize) {
        var currentItem: (Long, Long, Long) = allCats.get(i)
        // No1:  商品ID=12224  浏览量=2413
        result.append("No").append(i).append(":").append("  商品类目ID=").append(currentItem._1).append("  数量=").append(currentItem._3).append("\n")
        i += 1
      }
      result.append("====================================\n\n")
      // 控制输出频率，模拟实时滚动结果
      Thread.sleep(1000)
      out.collect(result.toString)
    }

  }

  class CountAgg extends AggregateFunction[(Long, Long, Long, String, Long), Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(userBehavior: (Long, Long, Long, String, Long), acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc1: Long, acc2: Long): Long = acc1 + acc2
  }

  class WindowResultFunction extends WindowFunction[Long, (Long, Long, Long), Tuple, TimeWindow] {
    override def apply(key: Tuple, window: TimeWindow, aggregateResult: Iterable[Long], collector: Collector[(Long, Long, Long)]) : Unit = {
      val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0
      val count = aggregateResult.iterator.next
      collector.collect((itemId, window.getEnd, count))
    }
  }
}