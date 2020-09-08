### 重定向迟到元素

迟到的元素也可以使用侧输出(side output)特性被重定向到另外的一条流中去。迟到元素所组成的侧输出流可以继续处理或者sink到持久化设施中去。

例子

```java
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

```java
class CountFunction extends ProcessWindowFunction[(String, Long),
  String, String, TimeWindow] {
  @Override
public process(key: String,
                       context: Context,
                       elements: Iterable[(String, Long)],
                       out: Collector[String]): Unit = {
    out.collect("窗口共有" + elements.size + "条数据")
  }
}
```

下面这个例子展示了ProcessFunction如何过滤掉迟到的元素然后将迟到的元素发送到侧输出流中去。

```java
val readings: DataStream[SensorReading] = ???
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

  @Override
public processElement(
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

