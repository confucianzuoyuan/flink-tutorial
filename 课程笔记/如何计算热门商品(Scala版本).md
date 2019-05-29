知识点：

1. 如何基于`EventTime`处理，如何指定`Watermark`
2. 如何使用`Flink`灵活的`Window API`
3. 何时需要用到`State`，以及如何使用
4. 如何使用`ProcessFunction`实现`TopN`功能

# 实战案例介绍

本案例将实现一个“实时热门商品”的需求，我们可以将“实时热门商品”翻译成程序员更好理解的需求：每隔5分钟输出最近一小时内点击量最多的前`N`个商品。将这个需求进行分解我们大概要做这么几件事情：

- 抽取出业务时间戳，告诉`Flink`框架基于业务时间做窗口
- 过滤出点击行为数据
- 按一小时的窗口大小，每5分钟统计一次，做滑动窗口聚合`（Sliding Window）`
- 按每个窗口聚合，输出每个窗口中点击量前`N`名的商品

# 数据准备

这里我们准备了一份淘宝用户行为数据集。本数据集包含了淘宝上某一天随机一百万用户的所有行为（包括点击、购买、加购、收藏）。数据集的组织形式和MovieLens-20M类似，即数据集的每一行表示一条用户行为，由用户ID、商品ID、商品类目ID、行为类型和时间戳组成，并以逗号分隔。关于数据集中每一列的详细描述如下：

列名称|说明
-----|---
用户ID|整数类型，加密后的用户ID
商品ID|整数类型，加密后的商品ID
商品类目ID|整数类型，加密后的商品所属类目ID
行为类型|字符串，枚举类型，包括(‘pv’, ‘buy’, ‘cart’, ‘fav’)
时间戳|行为发生的时间戳，单位秒

下载数据集到项目的`resources`目录下。

# 编写程序

在`src/main/scala`下创建`ScalaHotItemsWithoutKafka.scala`文件：

```scala
object ScalaHotItemsWithoutKafka {
    def main(args: Array[String]) : Unit = {
        
    }
}
```

与上文一样，我们会一步步往里面填充代码。第一步仍然是创建一个`StreamExecutionEnvironment`，我们把它添加到`main`函数中。

```java
val env = StreamExecutionEnvironment.getExecutionEnvironment
// 为了打印到控制台的结果不乱序，我们配置全局的并发为1，这里改变并发对结果正确性没有影响
env.setParallelism(1);
```

## 创建模拟数据源

在数据准备章节，我们已经将测试的数据集下载到本地了。是一个csv文件。

>注：虽然一个流式应用应该是一个一直运行着的程序，需要消费一个无限数据源。但是在本案例教程中，为了省去构建真实数据源的繁琐，我们使用了文件来模拟真实数据源，这并不影响下文要介绍的知识点。这也是一种本地验证`Flink`应用程序正确性的常用方式。

我们先创建一个`UserBehavior`的`case class`类。

```scala
/** 用户行为数据结构 **/
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)
```

读取`csv`文件并将每一行转成指定`UserBehavior case class`类型。

```scala
    val resourcesPath = getClass.getResource("/UserBehavior.csv")
    val stream = env
      .readTextFile(resourcesPath.getPath)
      .map(line => {
        val linearray = line.split(",")
        UserBehavior(linearray(0).toLong, linearray(1).toLong, linearray(2).toInt, linearray(3), linearray(4).toLong)
      })
```

## EventTime 与 Watermark

当我们说“统计过去一小时内点击量”，这里的“一小时”是指什么呢？ 在`Flink`中它可以是指`ProcessingTime`，也可以是`EventTime`，由用户决定。

- ProcessingTime：事件被处理的时间。也就是由机器的系统时间来决定。
- EventTime：事件发生的时间。一般就是数据本身携带的时间。

在本案例中，我们需要统计业务时间上的每小时的点击量，所以要基于`EventTime`来处理。那么如果让`Flink`按照我们想要的业务时间来处理呢？这里主要有两件事情要做。

第一件是告诉`Flink`我们现在按照`EventTime`模式进行处理，`Flink`默认使用`ProcessingTime`处理，所以我们要显式设置下。

```scala
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
```

第二件事情是指定如何获得业务时间，以及生成`Watermark`。`Watermark`是用来追踪业务事件的概念，可以理解成`EventTime`世界中的时钟，用来指示当前处理到什么时刻的数据了。由于我们的数据源的数据已经经过整理，没有乱序，即事件的时间戳是单调递增的，所以可以将每条数据的业务时间就当做`Watermark`。这里我们用 `assignAscendingTimestamps`来实现时间戳的抽取和`Watermark`的生成。

>注：真实业务场景一般都是存在乱序的，所以一般使用`BoundedOutOfOrdernessTimestampExtractor`。

```scala
.assignAscendingTimestamps(_.timestamp * 1000)
```

这样我们就得到了一个带有时间标记的数据流了，后面就能做一些窗口的操作。

## 过滤出点击事件

在开始窗口操作之前，先回顾下需求“每隔5分钟输出过去一小时内点击量最多的前`N`个商品”。由于原始数据中存在点击、加购、购买、收藏各种行为的数据，但是我们只需要统计点击量，所以先使用`filter`将点击行为数据过滤出来。

```scala
.filter(_.behavior.equals("pv"))
```

## 窗口统计点击量

由于要每隔5分钟统计一次最近一小时每个商品的点击量，所以窗口大小是一小时，每隔5分钟滑动一次。即分别要统计`[09:00, 10:00), [09:05, 10:05), [09:10, 10:10)…`等窗口的商品点击量。是一个常见的滑动窗口需求（Sliding Window）。

```java
val windowedData : DataStream<ItemViewCount> = pvData
    .keyBy("itemId")
    .timeWindow(Time.minutes(60), Time.minutes(5))
    .aggregate(new CountAgg(), new WindowResultFunction());
```

我们使用`.keyBy("itemId")`对商品进行分组，使用`.timeWindow(Time size, Time slide)`对每个商品做滑动窗口（1小时窗口，5分钟滑动一次）。然后我们使用 `.aggregate(AggregateFunction af, WindowFunction wf)`做增量的聚合操作，它能使用`AggregateFunction`提前聚合掉数据，减少`state`的存储压力。较之`.apply(WindowFunction wf)`会将窗口中的数据都存储下来，最后一起计算要高效地多。这里的`CountAgg`实现了`AggregateFunction`接口，功能是统计窗口中的条数，即遇到一条数据就加一。

```scala
/** COUNT 统计的聚合函数实现，每出现一条记录加一 */
  class CountAgg extends AggregateFunction[UserBehavior, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(userBehavior: UserBehavior, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc1: Long, acc2: Long): Long = acc1 + acc2
  }
```

`.aggregate(AggregateFunction af, WindowFunction wf)`的第二个参数`WindowFunction`将每个`key`每个窗口聚合后的结果带上其他信息进行输出。我们这里实现的`WindowResultFunction`将<主键商品ID，窗口，点击量>封装成了`ItemViewCount`进行输出。

```java
/** 用于输出窗口的结果 */
  class WindowResultFunction extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
    override def apply(key: Tuple, window: TimeWindow, aggregateResult: Iterable[Long], collector: Collector[ItemViewCount]) : Unit = {
      val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0
      val count = aggregateResult.iterator.next
      collector.collect(ItemViewCount(itemId, window.getEnd, count))
    }
  }

/** 商品点击量(窗口操作的输出类型) */
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)
```

现在我们得到了每个商品在每个窗口的点击量的数据流。

## TopN 计算最热门商品

为了统计每个窗口下最热门的商品，我们需要再次按窗口进行分组，这里根据`ItemViewCount中的windowEnd进行keyBy()`操作。然后使用`ProcessFunction`实现一个自定义的`TopN`函数`TopNHotItems`来计算点击量排名前3名的商品，并将排名结果格式化成字符串，便于后续输出。

```scala
val topItems : DataStream<String> = windowedData
    .keyBy("windowEnd")
    .process(new TopNHotItems(3));  // 求点击量前3名的商品
```

`ProcessFunction`是`Flink`提供的一个`low-level API`，用于实现更高级的功能。它主要提供了定时器`timer`的功能（支持`EventTime`或`ProcessingTime`）。本案例中我们将利用`timer`来判断何时收齐了某个`window`下所有商品的点击量数据。由于`Watermark`的进度是全局的，在`processElement`方法中，每当收到一条数据`ItemViewCount`，我们就注册一个`windowEnd+1`的定时器（`Flink`框架会自动忽略同一时间的重复注册）。`windowEnd+1`的定时器被触发时，意味着收到了`windowEnd+1`的`Watermark`，即收齐了该`windowEnd`下的所有商品窗口统计值。我们在`onTimer()`中处理将收集的所有商品及点击量进行排序，选出`TopN`，并将排名信息格式化成字符串后进行输出。

这里我们还使用了`ListState<ItemViewCount>`来存储收到的每条`ItemViewCount`消息，保证在发生故障时，状态数据的不丢失和一致性。`ListState`是`Flink`提供的类似`Java List`接口的`State API`，它集成了框架的`checkpoint`机制，自动做到了`exactly-once`的语义保证。

```scala
/** 求某个窗口中前 N 名的热门点击商品，key 为窗口时间戳，输出为 TopN 的结果字符串 */
  class TopNHotItems extends KeyedProcessFunction[Tuple, ItemViewCount, String] {
    private var topSize = 0

    def this(topSize: Int) {
      this()
      this.topSize = topSize
    }

    private var itemState : ListState[ItemViewCount] = null

    @throws[Exception]
    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      // 命名状态变量的名字和状态变量的类型
      val itemsStateDesc = new ListStateDescriptor[ItemViewCount]("itemState-state", classOf[ItemViewCount])
      // 从运行时上下文中获取状态并赋值
      itemState = getRuntimeContext.getListState(itemsStateDesc)
    }

    @throws[Exception]
    override def processElement(input: ItemViewCount, context: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, collector: Collector[String]): Unit = { // 每条数据都保存到状态中
      itemState.add(input)
      // 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有商品数据
      // 也就是当程序看到windowend + 1的水位线watermark时，触发onTimer回调函数
      context.timerService.registerEventTimeTimer(input.windowEnd + 1)
    }

    @throws[Exception]
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = { // 获取收到的所有商品点击量
      var allItems: List[ItemViewCount] = new ArrayList[ItemViewCount]
      import scala.collection.JavaConversions._
      for (item <- itemState.get) {
        allItems.add(item)
      }
      // 提前清除状态中的数据，释放空间
      itemState.clear()
      // 按照点击量从大到小排序
      allItems.sort(new Comparator[ItemViewCount]() {
        override def compare(o1: ItemViewCount, o2: ItemViewCount): Int = (o2.count - o1.count).toInt
      })
      // 将排名信息格式化成 String, 便于打印
      var result: StringBuilder = new StringBuilder
      result.append("====================================\n")
      result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")
      var i: Int = 0
      while (i < allItems.size && i < topSize) {
        var currentItem: ItemViewCount = allItems.get(i)
        // No1:  商品ID=12224  浏览量=2413
        result.append("No").append(i).append(":").append("  商品ID=").append(currentItem.itemId).append("  浏览量=").append(currentItem.count).append("\n")
        i += 1
      }
      result.append("====================================\n\n")
      // 控制输出频率，模拟实时滚动结果
      Thread.sleep(1000)
      out.collect(result.toString)
    }

  }
```

## 打印输出

最后一步我们将结果打印输出到控制台，并调用env.execute执行任务。

```scala
topItems.print();
env.execute("Hot Items Job");
```

# 运行程序

直接运行`main`函数，就能看到不断输出的每个时间点的热门商品ID。

# 总结

通过实现一个“实时热门商品”的案例，学习和实践了`Flink`的多个核心概念和`API`用法。包括`EventTime、Watermark`的使用，`State`的使用，`Window API`的使用，以及`TopN`的实现。

# 完整代码

```java
import java.sql.Timestamp
import java.util.{ArrayList, Comparator, List}

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

object ScalaHotItemsWithoutKafka {

  def main(args: Array[String]): Unit = {
    val resourcesPath = getClass.getResource("/UserBehavior.csv")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val stream = env
      .readTextFile(resourcesPath.getPath)
      .map(line => {
        val linearray = line.split(",")
        UserBehavior(linearray(0).toLong, linearray(1).toLong, linearray(2).toInt, linearray(3), linearray(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000)
      .filter(_.behavior.equals("pv"))
      .keyBy("itemId")
      .timeWindow(Time.minutes(60), Time.minutes(5))
      .aggregate(new CountAgg(), new WindowResultFunction())
      .keyBy(1)
      .process(new TopNHotItems(3))
      .print()

    env.execute("Hot Items Job")
  }

  class TopNHotItems extends KeyedProcessFunction[Tuple, ItemViewCount, String] {
    private var topSize = 0

    def this(topSize: Int) {
      this()
      this.topSize = topSize
    }

    private var itemState : ListState[ItemViewCount] = null

    @throws[Exception]
    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      // 命名状态变量的名字和状态变量的类型
      val itemsStateDesc = new ListStateDescriptor[ItemViewCount]("itemState-state", classOf[ItemViewCount])
      // 从运行时上下文中获取状态并赋值
      itemState = getRuntimeContext.getListState(itemsStateDesc)
    }

    @throws[Exception]
    override def processElement(input: ItemViewCount, context: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, collector: Collector[String]): Unit = { // 每条数据都保存到状态中
      itemState.add(input)
      // 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有商品数据
      // 也就是当程序看到windowend + 1的水位线watermark时，触发onTimer回调函数
      context.timerService.registerEventTimeTimer(input.windowEnd + 1)
    }

    @throws[Exception]
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = { // 获取收到的所有商品点击量
      var allItems: List[ItemViewCount] = new ArrayList[ItemViewCount]
      import scala.collection.JavaConversions._
      for (item <- itemState.get) {
        allItems.add(item)
      }
      // 提前清除状态中的数据，释放空间
      itemState.clear()
      // 按照点击量从大到小排序
      allItems.sort(new Comparator[ItemViewCount]() {
        override def compare(o1: ItemViewCount, o2: ItemViewCount): Int = (o2.count - o1.count).toInt
      })
      // 将排名信息格式化成 String, 便于打印
      var result: StringBuilder = new StringBuilder
      result.append("====================================\n")
      result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")
      var i: Int = 0
      while (i < allItems.size && i < topSize) {
        var currentItem: ItemViewCount = allItems.get(i)
        // No1:  商品ID=12224  浏览量=2413
        result.append("No").append(i).append(":").append("  商品ID=").append(currentItem.itemId).append("  浏览量=").append(currentItem.count).append("\n")
        i += 1
      }
      result.append("====================================\n\n")
      // 控制输出频率，模拟实时滚动结果
      Thread.sleep(1000)
      out.collect(result.toString)
    }

  }

  class CountAgg extends AggregateFunction[UserBehavior, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(userBehavior: UserBehavior, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc1: Long, acc2: Long): Long = acc1 + acc2
  }

  class WindowResultFunction extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
    override def apply(key: Tuple, window: TimeWindow, aggregateResult: Iterable[Long], collector: Collector[ItemViewCount]) : Unit = {
      val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0
      val count = aggregateResult.iterator.next
      collector.collect(ItemViewCount(itemId, window.getEnd, count))
    }
  }
}
```
