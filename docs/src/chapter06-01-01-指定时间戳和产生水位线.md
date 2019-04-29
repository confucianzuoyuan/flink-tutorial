### 指定时间戳和产生水位线

如果使用事件时间，那么流中的事件必须包含这个事件真正发生的时间。使用了事件时间的流必须携带水位线。

时间戳和水位线的单位是毫秒，记时从`1970-01-01T00:00:00Z`开始。到达某个操作符的水位线就会告知这个操作符：小于等于水位线中携带的时间戳的事件都已经到达这个操作符了。时间戳和水位线可以由`SourceFunction`产生，或者由用户自定义的时间戳分配器和水位线产生器来生成。

Flink暴露了TimestampAssigner接口供我们实现，使我们可以自定义如何从事件数据中抽取时间戳。一般来说，时间戳分配器需要在source操作符后马上进行调用。

>因为时间戳分配器看到的元素的顺序应该和source操作符产生数据的顺序是一样的，否则就乱了。这就是为什么我们经常将source操作符的并行度设置为1的原因。

也就是说，任何分区操作都会将元素的顺序打乱，例如：并行度改变，keyBy()操作等等。

所以最佳实践是：在尽量接近数据源source操作符的地方分配时间戳和产生水位线，甚至最好在SourceFunction中分配时间戳和产生水位线。当然在分配时间戳和产生水位线之前可以对流进行map和filter操作是没问题的，也就是说必须是窄依赖。

以下这种写法是可以的。

```java
DataStream<T> stream = env
  .addSource(...)
  .map(...)
  .filter(...)
  .assignTimestampsAndWatermarks(...)
```

下面的例子展示了首先filter流，然后再分配时间戳和水位线。


```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment;

// 从调用时刻开始给env创建的每一个stream追加时间特征
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

DataStream<SensorReading> readings = env
  .addSource(new SensorSource)
  .filter(r -> r.temperature > 25)
  .assignTimestampsAndWatermarks(new MyAssigner());
```

MyAssigner有两种类型

* AssignerWithPeriodicWatermarks
* AssignerWithPunctuatedWatermarks

以上两个接口都继承自TimestampAssigner。
