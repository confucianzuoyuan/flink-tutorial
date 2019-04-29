## 实时流量统计

* 基本需求
  * 从web服务器的日志中，统计实时的访问流量
  * 统计每分钟的ip访问量，取出访问量最大的5个地址，每5秒更新一次
* 解决思路
  * 将apache服务器日志中的时间，转换为时间戳，作为Event Time
  * 构建滑动窗口，窗口长度为1分钟，滑动距离为5秒

*数据准备*

将apache服务器的日志文件apache.log复制到资源文件目录src/main/resources下，我们将从这里读取数据。

*代码实现*

我们现在要实现的模块是“实时流量统计”。对于一个电商平台而言，用户登录的入口流量、不同页面的访问流量都是值得分析的重要数据，而这些数据，可以简单地从web服务器的日志中提取出来。我们在这里实现最基本的“页面浏览数”的统计，也就是读取服务器日志中的每一行log，统计在一段时间内用户访问url的次数。

具体做法为：每隔5秒，输出最近10分钟内访问量最多的前N个URL。可以看出，这个需求与之前“实时热门商品统计”非常类似，所以我们完全可以借鉴此前的代码。

完整代码如下：

```scala
object ApacheLogAnalysis {

  case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

  case class UrlViewCount(url: String, windowEnd: Long, count: Long)

  def main(args: Array[String]): Unit = {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val stream = env
      // 文件的绝对路径
      .readTextFile("apache.log的绝对路径")
      .map(line => {
        val linearray = line.split(" ")
        // 把时间戳ETL成毫秒
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp = simpleDateFormat.parse(linearray(3)).getTime
        ApacheLogEvent(linearray(0),
                       linearray(2),
                       timestamp,
                       linearray(5),
                       linearray(6))
      })
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](
          Time.milliseconds(1000)
        ) {
          override def extractTimestamp(t: ApacheLogEvent): Long = {
            t.eventTime
          }
        }
      )
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .aggregate(new CountAgg(), new WindowResultFunction())
      .keyBy(_.windowEnd)
      .process(new TopNHotUrls(5))
      .print()

    env.execute("Traffic Analysis Job")
  }

  class CountAgg extends AggregateFunction[ApacheLogEvent, Long, Long] {
    override def createAccumulator(): Long = 0L
    override def add(apacheLogEvent: ApacheLogEvent, acc: Long): Long = acc + 1
    override def getResult(acc: Long): Long = acc
    override def merge(acc1: Long, acc2: Long): Long = acc1 + acc2
  }

  class WindowResultFunction
    extends ProcessWindowFunction[Long, UrlViewCount, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
      out.collect(UrlViewCount(key, context.window.getEnd, elements.iterator.next()))
    }
  }

  class TopNHotUrls(topSize: Int)
    extends KeyedProcessFunction[Long, UrlViewCount, String] {

    lazy val urlState = getRuntimeContext.getListState(
      new ListStateDescriptor[UrlViewCount](
        "urlState-state",
        Types.of[UrlViewCount]
      )
    )

    override def processElement(
      input: UrlViewCount,
      context: KeyedProcessFunction[Long, UrlViewCount, String]#Context,
      collector: Collector[String]
    ): Unit = {
      // 每条数据都保存到状态中
      urlState.add(input)
      context
        .timerService
        .registerEventTimeTimer(input.windowEnd + 1)
    }

    override def onTimer(
      timestamp: Long,
      ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext,
      out: Collector[String]
    ): Unit = {
      // 获取收到的所有URL访问量
      val allUrlViews: ListBuffer[UrlViewCount] = ListBuffer()
      import scala.collection.JavaConversions._
      for (urlView <- urlState.get) {
        allUrlViews += urlView
      }
      // 提前清除状态中的数据，释放空间
      urlState.clear()
      // 按照访问量从大到小排序
      val sortedUrlViews = allUrlViews.sortBy(_.count)(Ordering.Long.reverse)
        .take(topSize)
      // 将排名信息格式化成 String, 便于打印
      var result: StringBuilder = new StringBuilder
      result
        .append("====================================\n")
        .append("时间: ")
        .append(new Timestamp(timestamp - 1))
        .append("\n")

      for (i <- sortedUrlViews.indices) {
        val currentUrlView: UrlViewCount = sortedUrlViews(i)
        // e.g.  No1：  URL=/blog/tags/firefox?flav=rss20  流量=55
        result
          .append("No")
          .append(i + 1)
          .append(": ")
          .append("  URL=")
          .append(currentUrlView.url)
          .append("  流量=")
          .append(currentUrlView.count)
          .append("\n")
      }
      result
        .append("====================================\n\n")
      // 控制输出频率，模拟实时滚动结果
      Thread.sleep(1000)
      out.collect(result.toString)
    }
  }
}
```

