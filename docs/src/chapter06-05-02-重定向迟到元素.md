### 重定向迟到元素

迟到的元素也可以使用侧输出(side output)特性被重定向到另外的一条流中去。迟到元素所组成的侧输出流可以继续处理或者sink到持久化设施中去。

例子

**scala version**

```scala
val readings = env
  .socketTextStream("localhost", 9999, '\n')
  .map(line => {
    val arr = line.split(" ")
    (arr(0), arr(1).toLong * 1000)
  })
  .assignAscendingTimestamps(_._2)

val countPer10Secs = readings
  .keyBy(_._1)
  .timeWindow(Time.seconds(10))
  .sideOutputLateData(
    new OutputTag[(String, Long)]("late-readings")
  )
  .process(new CountFunction())

val lateStream = countPer10Secs
  .getSideOutput(
    new OutputTag[(String, Long)]("late-readings")
  )

lateStream.print()
```

实现`CountFunction`:

```scala
class CountFunction extends ProcessWindowFunction[(String, Long),
  String, String, TimeWindow] {
  override def process(key: String,
                       context: Context,
                       elements: Iterable[(String, Long)],
                       out: Collector[String]): Unit = {
    out.collect("窗口共有" + elements.size + "条数据")
  }
}
```

**java version**

```java
public class RedirectLateEvent {

    private static OutputTag<Tuple2<String, Long>> output = new OutputTag<Tuple2<String, Long>>("late-readings"){};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple2<String, Long>> stream = env
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String s) throws Exception {
                        String[] arr = s.split(" ");
                        return Tuple2.of(arr[0], Long.parseLong(arr[1]) * 1000L);
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.
                                // like scala: assignAscendingTimestamps(_._2)
                                <Tuple2<String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> value, long l) {
                                        return value.f1;
                                    }
                                })
                );

        SingleOutputStreamOperator<String> lateReadings = stream
                .keyBy(r -> r.f0)
                .timeWindow(Time.seconds(5))
                .sideOutputLateData(output) // use after keyBy and timeWindow
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, Long>> iterable, Collector<String> collector) throws Exception {
                        long exactSizeIfKnown = iterable.spliterator().getExactSizeIfKnown();
                        collector.collect(exactSizeIfKnown + " of elements");
                    }
                });

        lateReadings.print();
        lateReadings.getSideOutput(output).print();

        env.execute();
    }
}
```

下面这个例子展示了ProcessFunction如何过滤掉迟到的元素然后将迟到的元素发送到侧输出流中去。

**scala version**

```scala
val readings: DataStream[SensorReading] = ...
val filteredReadings: DataStream[SensorReading] = readings
  .process(new LateReadingsFilter)

// retrieve late readings
val lateReadings: DataStream[SensorReading] = filteredReadings
  .getSideOutput(new OutputTag[SensorReading]("late-readings"))


/** A ProcessFunction that filters out late sensor readings and
  * re-directs them to a side output */
class LateReadingsFilter
    extends ProcessFunction[SensorReading, SensorReading] {

  val lateReadingsOut = new OutputTag[SensorReading]("late-readings")

  override def processElement(
      SensorReading r,
      ctx: ProcessFunction[SensorReading, SensorReading]#Context,
      out: Collector[SensorReading]): Unit = {

    // compare record timestamp with current watermark
    if (r.timestamp < ctx.timerService().currentWatermark()) {
      // this is a late reading => redirect it to the side output
      ctx.output(lateReadingsOut, r)
    } else {
      out.collect(r)
    }
  }
}
```

**java version**

```java
public class RedirectLateEvent {

    private static OutputTag<String> output = new OutputTag<String>("late-readings"){};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Tuple2<String, Long>> stream = env
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String s) throws Exception {
                        String[] arr = s.split(" ");
                        return Tuple2.of(arr[0], Long.parseLong(arr[1]) * 1000L);
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.
                                <Tuple2<String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> value, long l) {
                                        return value.f1;
                                    }
                                })
                )
                .process(new ProcessFunction<Tuple2<String, Long>, Tuple2<String, Long>>() {
                    @Override
                    public void processElement(Tuple2<String, Long> stringLongTuple2, Context context, Collector<Tuple2<String, Long>> collector) throws Exception {
                        if (stringLongTuple2.f1 < context.timerService().currentWatermark()) {
                            context.output(output, "late event is comming!");
                        } else {
                            collector.collect(stringLongTuple2);
                        }

                    }
                });

        stream.print();
        stream.getSideOutput(output).print();

        env.execute();
    }
}
```
