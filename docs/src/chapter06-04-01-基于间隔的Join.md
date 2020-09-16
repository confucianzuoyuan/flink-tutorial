### 基于间隔的Join

基于间隔的Join会对两条流中拥有相同键值以及彼此之间时间戳不超过某一指定间隔的事件进行Join。

下图展示了两条流（A和B）上基于间隔的Join，如果B中事件的时间戳相较于A中事件的时间戳不早于1小时且不晚于15分钟，则会将两个事件Join起来。Join间隔具有对称性，因此上面的条件也可以表示为A中事件的时间戳相较B中事件的时间戳不早于15分钟且不晚于1小时。

![](images/spaf_0607.png)

基于间隔的Join目前只支持事件时间以及INNER JOIN语义（无法发出未匹配成功的事件）。下面的例子定义了一个基于间隔的Join。

```scala
input1
  .intervalJoin(input2)
  .between(<lower-bound>, <upper-bound>) // 相对于input1的上下界
  .process(ProcessJoinFunction) // 处理匹配的事件对
```

Join成功的事件对会发送给ProcessJoinFunction。下界和上界分别由负时间间隔和正时间间隔来定义，例如between(Time.hour(-1), Time.minute(15))。在满足下界值小于上界值的前提下，你可以任意对它们赋值。例如，允许出现B中事件的时间戳相较A中事件的时间戳早1～2小时这样的条件。

基于间隔的Join需要同时对双流的记录进行缓冲。对第一个输入而言，所有时间戳大于当前水位线减去间隔上界的数据都会被缓冲起来；对第二个输入而言，所有时间戳大于当前水位线加上间隔下界的数据都会被缓冲起来。注意，两侧边界值都有可能为负。上图中的Join需要存储数据流A中所有时间戳大于当前水位线减去15分钟的记录，以及数据流B中所有时间戳大于当前水位线减去1小时的记录。不难想象，如果两条流的事件时间不同步，那么Join所需的存储就会显著增加，因为水位线总是由“较慢”的那条流来决定。

例子：每个用户的点击Join这个用户最近10分钟内的浏览

**scala version**

```scala
object IntervalJoinExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    /*
    A.intervalJoin(B).between(lowerBound, upperBound)
    B.intervalJoin(A).between(-upperBound, -lowerBound)
     */

    val stream1 = env
      .fromElements(
        ("user_1", 10 * 60 * 1000L, "click"),
        ("user_1", 16 * 60 * 1000L, "click")
      )
      .assignAscendingTimestamps(_._2)
      .keyBy(r => r._1)

    val stream2 = env
      .fromElements(
        ("user_1", 5 * 60 * 1000L, "browse"),
        ("user_1", 6 * 60 * 1000L, "browse")
      )
      .assignAscendingTimestamps(_._2)
      .keyBy(r => r._1)

    stream1
      .intervalJoin(stream2)
      .between(Time.minutes(-10), Time.minutes(0))
      .process(new ProcessJoinFunction[(String, Long, String), (String, Long, String), String] {
        override def processElement(in1: (String, Long, String), in2: (String, Long, String), context: ProcessJoinFunction[(String, Long, String), (String, Long, String), String]#Context, collector: Collector[String]): Unit = {
          collector.collect(in1 + " => " + in2)
        }
      })
      .print()

    stream2
      .intervalJoin(stream1)
      .between(Time.minutes(0), Time.minutes(10))
      .process(new ProcessJoinFunction[(String, Long, String), (String, Long, String), String] {
        override def processElement(in1: (String, Long, String), in2: (String, Long, String), context: ProcessJoinFunction[(String, Long, String), (String, Long, String), String]#Context, collector: Collector[String]): Unit = {
          collector.collect(in1 + " => " + in2)
        }
      })
      .print()

    env.execute()
  }
}
```

**java version**

```java
public class IntervalJoinExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        KeyedStream<Tuple3<String, Long, String>, String> stream1 = env
                .fromElements(
                        Tuple3.of("user_1", 10 * 60 * 1000L, "click")
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<String, Long, String>>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Long, String>>() {
                                    @Override
                                    public long extractTimestamp(Tuple3<String, Long, String> stringLongStringTuple3, long l) {
                                        return stringLongStringTuple3.f1;
                                    }
                                })
                )
                .keyBy(r -> r.f0);

        KeyedStream<Tuple3<String, Long, String>, String> stream2 = env
                .fromElements(
                        Tuple3.of("user_1", 5 * 60 * 1000L, "browse"),
                        Tuple3.of("user_1", 6 * 60 * 1000L, "browse")
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<String, Long, String>>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Long, String>>() {
                                    @Override
                                    public long extractTimestamp(Tuple3<String, Long, String> stringLongStringTuple3, long l) {
                                        return stringLongStringTuple3.f1;
                                    }
                                })
                )
                .keyBy(r -> r.f0);

        stream1
                .intervalJoin(stream2)
                .between(Time.minutes(-10), Time.minutes(0))
                .process(new ProcessJoinFunction<Tuple3<String, Long, String>, Tuple3<String, Long, String>, String>() {
                    @Override
                    public void processElement(Tuple3<String, Long, String> stringLongStringTuple3, Tuple3<String, Long, String> stringLongStringTuple32, Context context, Collector<String> collector) throws Exception {
                        collector.collect(stringLongStringTuple3 + " => " + stringLongStringTuple32);
                    }
                })
                .print();

        env.execute();

    }
}
```
