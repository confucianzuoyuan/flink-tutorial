## 实时热门商品统计

首先要实现的是实时热门商品统计，我们将会基于UserBehavior数据集来进行分析。

*基本需求*

* 每隔5分钟输出最近一小时内点击量最多的前N个商品
* 点击量用浏览次数("pv")来衡量

*解决思路*

* 在所有用户行为数据中，过滤出浏览("pv")行为进行统计
* 构建滑动窗口，窗口长度为1小时，滑动距离为5分钟
* 窗口计算使用增量聚合函数和全窗口聚合函数相结合的方法
* 使用窗口结束时间作为key，对DataStream进行keyBy()操作
* 将KeyedStream中的元素存储到ListState中，当水位线超过窗口结束时间时，排序输出

*数据准备*

将数据文件UserBehavior.csv复制到资源文件目录src/main/resources下。

*程序主体*

**scala version**

```scala
// 把数据需要ETL成UserBehavior类型
case class UserBehavior(userId: Long,
                        itemId: Long,
                        categoryId: Int,
                        behavior: String,
                        timestamp: Long)

// 全窗口聚合函数输出的数据类型
case class ItemViewCount(itemId: Long,
                         windowEnd: Long,
                         count: Long)

object HotItems {
  def main(args: Array[String]): Unit = {
    // 创建一个 StreamExecutionEnvironment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设定Time类型为EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 为了打印到控制台的结果不乱序，
    // 我们配置全局的并发为1，这里改变并发对结果正确性没有影响
    env.setParallelism(1)
    val stream = env
      // 以window下为例，需替换成数据集的绝对路径
      .readTextFile("YOUR_PATH\\resources\\UserBehavior.csv")
      .map(line => {
        val linearray = line.split(",")
        UserBehavior(linearray(0).toLong,
                     linearray(1).toLong,
                     linearray(2).toInt,
                     linearray(3),
                     linearray(4).toLong)
      })
      // 过滤出点击事件
      .filter(_.behavior == "pv")
      // 指定时间戳和Watermark，这里我们已经知道了数据集的时间戳是单调递增的了。
      .assignAscendingTimestamps(_.timestamp * 1000)
      // 根据商品Id分流
      .keyBy(_.itemId)
      // 开窗操作
      .timeWindow(Time.minutes(60), Time.minutes(5))
      // 窗口计算操作
      .aggregate(new CountAgg(), new WindowResultFunction())
      // 根据窗口结束时间分流
      .keyBy(_.windowEnd)
      // 求点击量前3名的商品
      .process(new TopNHotItems(3))

    // 打印结果
    stream.print()

    // 别忘了执行
    env.execute("Hot Items Job")
  }
}
```

>真实业务场景一般都是乱序的，所以一般不用`assignAscendingTimestamps`，而是使用`BoundedOutOfOrdernessTimestampExtractor`。

*增量聚合函数逻辑编写*

```scala
// COUNT统计的聚合函数实现，每出现一条记录就加一
class CountAgg extends AggregateFunction[UserBehavior, Long, Long] {
  override def createAccumulator(): Long = 0L
  override def add(userBehavior: UserBehavior, acc: Long): Long = acc + 1
  override def getResult(acc: Long): Long = acc
  override def merge(acc1: Long, acc2: Long): Long = acc1 + acc2
}
```

*全窗口聚合函数逻辑编写*

其实就是将增量聚合的结果包上一层窗口信息和key的信息。

代码如下：

```scala
// 用于输出窗口的结果
class WindowResultFunction extends ProcessWindowFunction[Long, ItemViewCount, String, TimeWindow] {
  override def process(key: String,
                        context: Context,
                        elements: Iterable[Long],
                        out: Collector[ItemViewCount]): Unit = {
    out.collect(ItemViewCount(key, context.window.getEnd, elements.iterator.next()))
  }
}
```

现在我们就得到了每个商品在每个窗口的点击量的数据流。

*计算最热门TopN商品*

```scala
  class TopNHotItems(topSize: Int)
    extends KeyedProcessFunction[Long, ItemViewCount, String] {
    // 惰性赋值一个状态变量
    lazy val itemState = getRuntimeContext.getListState(
      new ListStateDescriptor[ItemViewCount]("items", Types.of[ItemViewCount])
    )

    // 来一条数据都会调用一次
    override def processElement(value: ItemViewCount,
                                ctx: KeyedProcessFunction[Long,
                                  ItemViewCount, String]#Context,
                                out: Collector[String]): Unit = {
      itemState.add(value)
      ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
    }

    // 定时器事件
    override def onTimer(
      ts: Long,
      ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext,
      out: Collector[String]
    ): Unit = {
      val allItems: ListBuffer[ItemViewCount] = ListBuffer()
      // 导入一些隐式类型转换
      import scala.collection.JavaConversions._
      for (item <- itemState.get) {
        allItems += item
      }

      // 清空状态变量，释放空间
      itemState.clear()

      // 降序排列
      val sortedItems = allItems.sortBy(-_.count).take(topSize)
      val result = new StringBuilder
      result.append("====================================\n")
      result.append("时间: ").append(new Timestamp(ts - 1)).append("\n")
      for (i <- sortedItems.indices) {
        val currentItem = sortedItems(i)
        result.append("No")
          .append(i+1)
          .append(":")
          .append("  商品ID=")
          .append(currentItem.itemId)
          .append("  浏览量=")
          .append(currentItem.count)
          .append("\n")
      }
      result.append("====================================\n\n")
      Thread.sleep(1000)
      out.collect(result.toString())
    }
  }
```

*更换Kafka作为数据源*

实际生产环境中，我们的数据流往往是从Kafka获取到的。如果要让代码更贴近生产实际，我们只需将source更换为Kafka即可：

编写代码：

```scala
val properties = new Properties()
properties.setProperty("bootstrap.servers", "localhost:9092")
properties.setProperty("group.id", "consumer-group")
properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
properties.setProperty("auto.offset.reset", "latest")

val env = StreamExecutionEnvironment.getExecutionEnvironment
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
env.setParallelism(1)

val stream = env
  .addSource(new FlinkKafkaConsumer[String](
    "hotitems",
    new SimpleStringSchema(),
    properties)
  )
```

当然，根据实际的需要，我们还可以将Sink指定为Kafka、ES、Redis或其它存储，这里就不一一展开实现了。

*kafka生产者程序*

```scala
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaProducerUtil {

  def main(args: Array[String]): Unit = {
    writeToKafka("hotitems")
  }

  def writeToKafka(topic: String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put(
      "key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer"
    )
    props.put(
      "value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer"
    )
    val producer = new KafkaProducer[String, String](props)
    val bufferedSource = io.Source.fromFile("UserBehavior.csv文件的绝对路径")
    for (line <- bufferedSource.getLines) {
      val record = new ProducerRecord[String, String](topic, line)
      producer.send(record)
    }
    producer.close()
  }
}
```

### java版本程序

UserBehavior的POJO类定义

```java
public class UserBehavior {
    public String userId;
    public String itemId;
    public String categoryId;
    public String behavior;
    public Long timestamp;

    public UserBehavior() {
    }

    public UserBehavior(String userId, String itemId, String categoryId, String behavior, Long timestamp) {
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.behavior = behavior;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "UserBehavior{" +
                "userId='" + userId + '\'' +
                ", itemId='" + itemId + '\'' +
                ", categoryId='" + categoryId + '\'' +
                ", behavior='" + behavior + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
```

ItemViewCount的POJO类定义

```java
import java.sql.Timestamp;

public class ItemViewCount {
    public String itemId;
    public Long count;
    public Long windowStart;
    public Long windowEnd;

    public ItemViewCount() {
    }

    public ItemViewCount(String itemId, Long count, Long windowStart, Long windowEnd) {
        this.itemId = itemId;
        this.count = count;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    @Override
    public String toString() {
        return "ItemViewCount{" +
                "itemId='" + itemId + '\'' +
                ", count=" + count +
                ", windowStart=" + new Timestamp(windowStart) +
                ", windowEnd=" + new Timestamp(windowEnd) +
                '}';
    }
}
```

主体程序

```java
public class UserBehaviorAnalysis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        SingleOutputStreamOperator<UserBehavior> stream = env
                .readTextFile("UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String s) throws Exception {
                        String[] arr = s.split(",");
                        return new UserBehavior(arr[0], arr[1], arr[2], arr[3], Long.parseLong(arr[4]) * 1000L);
                    }
                })
                .filter(r -> r.behavior.equals("pv"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                                    @Override
                                    public long extractTimestamp(UserBehavior userBehavior, long l) {
                                        return userBehavior.timestamp;
                                    }
                                })
                );

        stream
                .keyBy(r -> r.itemId)
                .timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new CountAgg(), new WindowResult())
                .keyBy(r -> r.windowEnd)
                .process(new TopN(3))
                .print();

        env.execute();
    }

    public static class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior userBehavior, Long acc) {
            return acc + 1;
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return null;
        }
    }

    public static class WindowResult extends ProcessWindowFunction<Long, ItemViewCount, String, TimeWindow> {
        @Override
        public void process(String key, Context ctx, Iterable<Long> iterable, Collector<ItemViewCount> collector) throws Exception {
            collector.collect(new ItemViewCount(key, iterable.iterator().next(), ctx.window().getStart(), ctx.window().getEnd()));
        }
    }

    public static class TopN extends KeyedProcessFunction<Long, ItemViewCount, String> {
        private ListState<ItemViewCount> itemState;
        private Integer threshold;

        public TopN(Integer threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            itemState = getRuntimeContext().getListState(
                    new ListStateDescriptor<ItemViewCount>("item-state", ItemViewCount.class)
            );
        }

        @Override
        public void processElement(ItemViewCount itemViewCount, Context context, Collector<String> collector) throws Exception {
            itemState.add(itemViewCount);
            context.timerService().registerEventTimeTimer(itemViewCount.windowEnd + 100L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            Iterable<ItemViewCount> itemViewCounts = itemState.get();
            ArrayList<ItemViewCount> itemList = new ArrayList<>();
            for (ItemViewCount iter : itemViewCounts) {
                itemList.add(iter);
            }
            itemState.clear();

            itemList.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount t0, ItemViewCount t1) {
                    return t1.count.intValue() - t0.count.intValue();
                }
            });

            StringBuilder result = new StringBuilder();
            result
                    .append("===========================================\n")
                    .append("time: ")
                    .append(new Timestamp(timestamp - 100L))
                    .append("\n");
            for (int i = 0; i < this.threshold; i++) {
                ItemViewCount currItem = itemList.get(i);
                result
                        .append("No.")
                        .append(i + 1)
                        .append(" : ")
                        .append(currItem.itemId)
                        .append(" count = ")
                        .append(currItem.count)
                        .append("\n");
            }
            result
                    .append("===========================================\n\n\n");
            Thread.sleep(1000L);
            out.collect(result.toString());
        }
    }
}
```

将用户行为数据写入`Kafka`

```java
public class UserBehaviorProduceToKafka {
    public static void main(String[] args) throws Exception {
        writeToKafka("hotitems");
    }

    public static void writeToKafka(String topic) throws Exception {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        // pass the path to the file as a parameter
        File file = new File("UserBehavior.csv");
        Scanner sc = new Scanner(file);

        while (sc.hasNextLine()) {
            producer.send(new ProducerRecord<String, String>(topic, sc.nextLine()));
        }
    }
}
```

别忘记导入驱动

```xml
<dependency>
	<groupId>org.apache.kafka</groupId>
	<artifactId>kafka_2.11</artifactId>
	<version>2.2.0</version>
</dependency>
```
