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

```java
class PeriodicAssigner extends AssignerWithPeriodicWatermarks[SensorReading] {
  val bound = 60 * 1000 // 延时为1分钟
  var maxTs = Long.MinValue + bound // 观察到的最大时间戳

  override def getCurrentWatermark: Watermark {
    new Watermark(maxTs - bound)
  }

  override def extractTimestamp(r: SensorReading, previousTS: Long) {
    maxTs = maxTs.max(r.timestamp)
    r.timestamp
  }
}
```

如果我们事先得知数据流的时间戳是单调递增的，也就是说没有乱序。我们可以使用assignAscendingTimestamps，方法会直接使用数据的时间戳生成水位线。

```scala
val stream = ...
val withTimestampsAndWatermarks = stream.assignAscendingTimestamps(e => e.timestamp)
```

如果我们能大致估算出数据流中的事件的最大延迟时间，可以使用如下代码：

>最大延迟时间就是当前到达的事件的事件时间和之前所有到达的事件中最大时间戳的差。

```scala
val stream = ...
val withTimestampsAndWatermarks = stream.assignTimestampsAndWatermarks(
  new SensorTimeAssigner()
)

class SensorTimeAssigner extends BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(5)) {
    // 抽取时间戳
    override def extractTimestamp(r: SensorReading): Long {
      r.timestamp;
    }
}
```

以上代码设置了最大延迟时间为5秒。

