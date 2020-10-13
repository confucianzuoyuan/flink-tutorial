## Uv统计的布隆过滤器实现

### UV实现的最简单版本

**java version**

```java
public class UvStatistics {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<UserBehavior> stream = env
                .readTextFile("/home/zuoyuan/flink-tutorial/flink-code-java-0421/src/main/resources/UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] arr = value.split(",");
                        return new UserBehavior(
                                arr[0],
                                arr[1],
                                arr[2],
                                arr[3],
                                Long.parseLong(arr[4]) * 1000L
                        );
                    }
                })
                .filter(r -> r.behavior.equals("pv"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserBehavior>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                                    @Override
                                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                );

        stream
                .map(new MapFunction<UserBehavior, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map(UserBehavior value) throws Exception {
                        return Tuple2.of("dummy", value.userId);
                    }
                })
                .keyBy(r -> r.f0)
                .timeWindow(Time.hours(1))
                .aggregate(new CountAgg(), new WindowResult())
                .print();

        env.execute();
    }

    public static class WindowResult extends ProcessWindowFunction<Long, String, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<Long> iterable, Collector<String> collector) throws Exception {
            collector.collect("窗口结束时间是：" + new Timestamp(context.window().getEnd()) + " 的窗口的UV是：" + iterable.iterator().next());
        }
    }

    public static class CountAgg implements AggregateFunction<Tuple2<String, String>, Tuple2<Set<String>, Long>, Long> {
        @Override
        public Tuple2<Set<String>, Long> createAccumulator() {
            return Tuple2.of(new HashSet<>(), 0L);
        }

        @Override
        public Tuple2<Set<String>, Long> add(Tuple2<String, String> value, Tuple2<Set<String>, Long> accumulator) {
            if (!accumulator.f0.contains(value.f1)) {
                accumulator.f0.add(value.f1);
                accumulator.f1 += 1;
            }
            return accumulator;
        }

        @Override
        public Long getResult(Tuple2<Set<String>, Long> accumulator) {
            return accumulator.f1;
        }

        @Override
        public Tuple2<Set<String>, Long> merge(Tuple2<Set<String>, Long> a, Tuple2<Set<String>, Long> b) {
            return null;
        }
    }
}
```

**scala version**

```scala
import scala.collection.mutable.Set

object UVPerWindowWithAggAndWindowProcess {

  case class UserBehavior(userId: String, itemId: String, categoryId: String, behavior: String, timestamp: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env
      .readTextFile("UserBehavior.csv")
      .map(line => {
        val arr = line.split(",")
        UserBehavior(arr(0), arr(1), arr(2), arr(3), arr(4).toLong * 1000L)
      })
      .filter(r => r.behavior.equals("pv"))
      .assignAscendingTimestamps(_.timestamp)

    stream
      .map(r => ("key", r.userId))
      .keyBy(r => r._1)
      .timeWindow(Time.hours(1))
      .aggregate(new CountAgg, new WindowResult)
      .print()

    env.execute()
  }

  class WindowResult extends ProcessWindowFunction[Long, String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[String]): Unit = {
      out.collect("window end: " + new Timestamp(context.window.getEnd) + " uv count: " + elements.head)
    }
  }

  class CountAgg extends AggregateFunction[(String, String), (Set[String], Long), Long] {
    override def createAccumulator(): (mutable.Set[String], Long) = (mutable.Set[String](), 0L)

    override def add(in: (String, String), acc: (mutable.Set[String], Long)): (mutable.Set[String], Long) = {
      if (!acc._1.contains(in._2)) {
        acc._1 += in._2
        (acc._1, acc._2 + 1)
      } else {
        acc
      }
    }

    override def getResult(acc: (mutable.Set[String], Long)): Long = acc._2

    override def merge(acc: (mutable.Set[String], Long), acc1: (mutable.Set[String], Long)): (mutable.Set[String], Long) = ???
  }
}
```

### 布隆过滤器版本

完整代码如下：

**scala version**

```scala
import org.apache.flink.shaded.guava18.com.google.common.hash.{BloomFilter, Funnels}

object UVPerWindowWithBloomFilter {
  case class UserBehavior(userId: String, itemId: String, categoryId: String, behavior: String, timestamp: Long)
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env
      .readTextFile("/home/zuoyuan/flink-tutorial/flink-scala-code/src/main/resources/UserBehavior.csv")
      .map(line => {
        val arr = line.split(",")
        UserBehavior(arr(0), arr(1), arr(2), arr(3), arr(4).toLong * 1000L)
      })
      .filter(r => r.behavior.equals("pv"))
      .assignAscendingTimestamps(_.timestamp)

    stream
      .map(r => ("key", r.userId.toLong))
      .keyBy(r => r._1)
      .timeWindow(Time.hours(1))
      .aggregate(new CountAgg, new WindowResult)
      .print()

    env.execute()
  }

  class CountAgg extends AggregateFunction[(String, Long), (Long, BloomFilter[lang.Long]), Long] {
    override def createAccumulator(): (Long, BloomFilter[lang.Long]) = (0, BloomFilter.create(Funnels.longFunnel(), 100000000, 0.01))

    override def add(in: (String, Long), acc: (Long, BloomFilter[lang.Long])): (Long, BloomFilter[lang.Long]) = {
      if (!acc._2.mightContain(in._2)) {
        acc._2.put(in._2)
        (acc._1 + 1, acc._2)
      } else {
        acc
      }
    }

    override def getResult(acc: (Long, BloomFilter[lang.Long])): Long = acc._1

    override def merge(acc: (Long, BloomFilter[lang.Long]), acc1: (Long, BloomFilter[lang.Long])): (Long, BloomFilter[lang.Long]) = ???

  }

  class WindowResult extends ProcessWindowFunction[Long, String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[String]): Unit = {
      out.collect("window end: " + new Timestamp(context.window.getEnd) + " uv count: " + elements.head)
    }
  }
}
```

**java version**

```java
public class UvStatisticsWithBloomFilter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<UserBehavior> stream = env
                .readTextFile("/home/zuoyuan/flink-tutorial/flink-code-java-0421/src/main/resources/UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] arr = value.split(",");
                        return new UserBehavior(
                                arr[0],
                                arr[1],
                                arr[2],
                                arr[3],
                                Long.parseLong(arr[4]) * 1000L
                        );
                    }
                })
                .filter(r -> r.behavior.equals("pv"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserBehavior>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                                    @Override
                                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                );

        stream
                .map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(UserBehavior value) throws Exception {
                        return Tuple2.of("dummy", Long.parseLong(value.userId));
                    }
                })
                .keyBy(r -> r.f0)
                .timeWindow(Time.hours(1))
                .aggregate(new CountAgg(), new WindowResult())
                .print();

        env.execute();
    }

    public static class WindowResult extends ProcessWindowFunction<Long, String, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<Long> iterable, Collector<String> collector) throws Exception {
            collector.collect("窗口结束时间是：" + new Timestamp(context.window().getEnd()) + " 的窗口的UV是：" + iterable.iterator().next());
        }
    }

    public static class CountAgg implements AggregateFunction<Tuple2<String, Long>, Tuple2<BloomFilter<Long>, Long>, Long> {
        @Override
        public Tuple2<BloomFilter<Long>, Long> createAccumulator() {
            // 去重的数据类型是Long，待去重的数据集大小是100万条数据以内，误判率0.01
            return Tuple2.of(BloomFilter.create(Funnels.longFunnel(), 1000000, 0.01), 0L);
        }

        @Override
        public Tuple2<BloomFilter<Long>, Long> add(Tuple2<String, Long> value, Tuple2<BloomFilter<Long>, Long> accumulator) {
            if (!accumulator.f0.mightContain(value.f1)) {
                accumulator.f0.put(value.f1);
                accumulator.f1 += 1;
            }
            return accumulator;
        }

        @Override
        public Long getResult(Tuple2<BloomFilter<Long>, Long> accumulator) {
            return accumulator.f1;
        }

        @Override
        public Tuple2<BloomFilter<Long>, Long> merge(Tuple2<BloomFilter<Long>, Long> a, Tuple2<BloomFilter<Long>, Long> b) {
            return null;
        }
    }
}
```
