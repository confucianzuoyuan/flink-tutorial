### 周期性的生成水位线

周期性的生成水位线：系统会周期性的将水位线插入到流中（水位线也是一种特殊的事件!）。默认周期是200毫秒，也就是说，系统会每隔200毫秒就往流中插入一次水位线。

>这里的200毫秒是机器时间！

可以使用`ExecutionConfig.setAutoWatermarkInterval()`方法进行设置。

```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
// 每隔5秒产生一个水位线
env.getConfig.setAutoWatermarkInterval(5000)
```

上面的例子产生水位线的逻辑：每隔5秒钟，Flink会调用AssignerWithPeriodicWatermarks中的getCurrentWatermark()方法。如果方法返回的时间戳大于之前水位线的时间戳，新的水位线会被插入到流中。这个检查保证了水位线是单调递增的。如果方法返回的时间戳小于等于之前水位线的时间戳，则不会产生新的水位线。

例子，自定义一个周期性的时间戳抽取

**scala version**

```scala
class PeriodicAssigner extends AssignerWithPeriodicWatermarks[SensorReading] {
  val bound = 60 * 1000 // 延时为1分钟
  var maxTs = Long.MinValue + bound + 1 // 观察到的最大时间戳

  override def getCurrentWatermark: Watermark {
    new Watermark(maxTs - bound - 1)
  }

  override def extractTimestamp(r: SensorReading, previousTS: Long) {
    maxTs = maxTs.max(r.timestamp)
    r.timestamp
  }
}
```

**java version**

```java
.assignTimestampsAndWatermarks(
  // generate periodic watermarks
  new AssignerWithPeriodicWatermarks[(String, Long)] {
    val bound = 10 * 1000L // 最大延迟时间
    var maxTs = Long.MinValue + bound + 1 // 当前观察到的最大时间戳

    // 用来生成水位线
    // 默认200ms调用一次
    override def getCurrentWatermark: Watermark = {
      println("generate watermark!!!" + (maxTs - bound - 1) + "ms")
      new Watermark(maxTs - bound - 1)
    }

    // 每来一条数据都会调用一次
    override def extractTimestamp(t: (String, Long), l: Long): Long = {
      println("extract timestamp!!!")
      maxTs = maxTs.max(t._2) // 更新观察到的最大事件时间
      t._2 // 抽取时间戳
    }
  }
)
```

如果我们事先得知数据流的时间戳是单调递增的，也就是说没有乱序。我们可以使用assignAscendingTimestamps，方法会直接使用数据的时间戳生成水位线。

**scala version**

```scala
val stream = ...
val withTimestampsAndWatermarks = stream.assignAscendingTimestamps(e => e.timestamp)
```

**java version**

```java
.assignTimestampsAndWatermarks(
        WatermarkStrategy
                .<SensorReading>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {
                    @Override
                    public long extractTimestamp(SensorReading r, long l) {
                        return r.timestamp;
                    }
                })
)
```

如果我们能大致估算出数据流中的事件的最大延迟时间，可以使用如下代码：

>最大延迟时间就是当前到达的事件的事件时间和之前所有到达的事件中最大时间戳的差。

**scala version**

```scala
.assignTimestampsAndWatermarks(
  // 水位线策略；默认200ms的机器时间插入一次水位线
  // 水位线 = 当前观察到的事件所携带的最大时间戳 - 最大延迟时间
  WatermarkStrategy
    // 最大延迟时间设置为5s
    .forBoundedOutOfOrderness[(String, Long)](Duration.ofSeconds(5))
    .withTimestampAssigner(new SerializableTimestampAssigner[(String, Long)] {
      // 告诉系统第二个字段是时间戳，时间戳的单位是毫秒
      override def extractTimestamp(element: (String, Long), recordTimestamp: Long): Long = element._2
    })
)
```

**java version**

```java
.assignTimestampsAndWatermarks(
        WatermarkStrategy
                .<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                        return element.f1;
                    }
                })
)
```

以上代码设置了最大延迟时间为5秒。

