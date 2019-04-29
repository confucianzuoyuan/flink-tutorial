### 调用窗口计算函数

window functions定义了窗口中数据的计算逻辑。有两种计算逻辑：

1. 增量聚合函数(Incremental aggregation functions)：当一个事件被添加到窗口时，触发函数计算，并且更新window的状态(单个值)。最终聚合的结果将作为输出。ReduceFunction和AggregateFunction是增量聚合函数。

2. 全窗口函数(Full window functions)：这个函数将会收集窗口中所有的元素，可以做一些复杂计算。ProcessWindowFunction是window function。

*ReduceFunction*

例子: 计算每个传感器15s窗口中的温度最小值

**scala version**

```scala
val minTempPerWindow = sensorData
  .map(r => (r.id, r.temperature))
  .keyBy(_._1)
  .timeWindow(Time.seconds(15))
  .reduce((r1, r2) => (r1._1, r1._2.min(r2._2)))
```

**java version**

```java
DataStream<Tuple2<String, Double>> minTempPerwindow = sensorData
    .map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
        @Override
        public Tuple2<String, Double> map(SensorReading value) throws Exception {
            return Tuple2.of(value.id, value.temperature);
        }
    })
    .keyBy(r -> r.f0)
    .timeWindow(Time.seconds(5))
    .reduce(new ReduceFunction<Tuple2<String, Double>>() {
        @Override
        public Tuple2<String, Double> reduce(Tuple2<String, Double> value1, Tuple2<String, Double> value2) throws Exception {
            if (value1.f1 < value2.f1) {
                return value1;
            } else {
                return value2;
            }
        }
    })
```

*AggregateFunction*

先来看接口定义

```java
public interface AggregateFunction<IN, ACC, OUT>
  extends Function, Serializable {

  // create a new accumulator to start a new aggregate
  ACC createAccumulator();

  // add an input element to the accumulator and return the accumulator
  ACC add(IN value, ACC accumulator);

  // compute the result from the accumulator and return it.
  OUT getResult(ACC accumulator);

  // merge two accumulators and return the result.
  ACC merge(ACC a, ACC b);
}
```

IN是输入元素的类型，ACC是累加器的类型，OUT是输出元素的类型。

例子

```java
val avgTempPerWindow: DataStream[(String, Double)] = sensorData
  .map(r => (r.id, r.temperature))
  .keyBy(_._1)
  .timeWindow(Time.seconds(15))
  .aggregate(new AvgTempFunction)

// An AggregateFunction to compute the average temperature per sensor.
// The accumulator holds the sum of temperatures and an event count.
class AvgTempFunction
  extends AggregateFunction[(String, Double),
    (String, Double, Int), (String, Double)] {

  override def createAccumulator() = {
    ("", 0.0, 0)
  }

  override def add(in: (String, Double), acc: (String, Double, Int)) = {
    (in._1, in._2 + acc._2, 1 + acc._3)
  }

  override def getResult(acc: (String, Double, Int)) = {
    (acc._1, acc._2 / acc._3)
  }

  override def merge(acc1: (String, Double, Int),
    acc2: (String, Double, Int)) = {
    (acc1._1, acc1._2 + acc2._2, acc1._3 + acc2._3)
  }
}
```

*ProcessWindowFunction*

一些业务场景，我们需要收集窗口内所有的数据进行计算，例如计算窗口数据的中位数，或者计算窗口数据中出现频率最高的值。这样的需求，使用ReduceFunction和AggregateFunction就无法实现了。这个时候就需要ProcessWindowFunction了。

先来看接口定义

```java
public abstract class ProcessWindowFunction<IN, OUT, KEY, W extends Window>
  extends AbstractRichFunction {

  // Evaluates the window
  void process(KEY key, Context ctx, Iterable<IN> vals, Collector<OUT> out)
    throws Exception;

  // Deletes any custom per-window state when the window is purged
  public void clear(Context ctx) throws Exception {}

  // The context holding window metadata
  public abstract class Context implements Serializable {
    // Returns the metadata of the window
    public abstract W window();

    // Returns the current processing time
    public abstract long currentProcessingTime();

    // Returns the current event-time watermark
    public abstract long currentWatermark();

    // State accessor for per-window state
    public abstract KeyedStateStore windowState();

    // State accessor for per-key global state
    public abstract KeyedStateStore globalState();

    // Emits a record to the side output identified by the OutputTag.
    public abstract <X> void output(OutputTag<X> outputTag, X value);
  }
}
```

process()方法接受的参数为：window的key，Iterable迭代器包含窗口的所有元素，Collector用于输出结果流。Context参数和别的process方法一样。而ProcessWindowFunction的Context对象还可以访问window的元数据(窗口开始和结束时间)，当前处理时间和水位线，per-window state和per-key global state，side outputs。

* per-window state: 用于保存一些信息，这些信息可以被process()访问，只要process所处理的元素属于这个窗口。
* per-key global state: 同一个key，也就是在一条KeyedStream上，不同的window可以访问per-key global state保存的值。

例子：计算5s滚动窗口中的最低和最高的温度。输出的元素包含了(流的Key, 最低温度, 最高温度, 窗口结束时间)。

```java
val minMaxTempPerWindow: DataStream[MinMaxTemp] = sensorData
  .keyBy(_.id)
  .timeWindow(Time.seconds(5))
  .process(new HighAndLowTempProcessFunction)

case class MinMaxTemp(id: String, min: Double, max: Double, endTs: Long)

class HighAndLowTempProcessFunction
  extends ProcessWindowFunction[SensorReading,
    MinMaxTemp, String, TimeWindow] {
  override def process(key: String,
                       ctx: Context,
                       vals: Iterable[SensorReading],
                       out: Collector[MinMaxTemp]): Unit = {
    val temps = vals.map(_.temperature)
    val windowEnd = ctx.window.getEnd

    out.collect(MinMaxTemp(key, temps.min, temps.max, windowEnd))
  }
}
```

我们还可以将ReduceFunction/AggregateFunction和ProcessWindowFunction结合起来使用。ReduceFunction/AggregateFunction做增量聚合，ProcessWindowFunction提供更多的对数据流的访问权限。如果只使用ProcessWindowFunction(底层的实现为将事件都保存在ListState中)，将会非常占用空间。分配到某个窗口的元素将被提前聚合，而当窗口的trigger触发时，也就是窗口收集完数据关闭时，将会把聚合结果发送到ProcessWindowFunction中，这时Iterable参数将会只有一个值，就是前面聚合的值。

例子

```java
input
  .keyBy(...)
  .timeWindow(...)
  .reduce(
    incrAggregator: ReduceFunction[IN],
    function: ProcessWindowFunction[IN, OUT, K, W])

input
  .keyBy(...)
  .timeWindow(...)
  .aggregate(
    incrAggregator: AggregateFunction[IN, ACC, V],
    windowFunction: ProcessWindowFunction[V, OUT, K, W])
```

我们把之前的需求重新使用以上两种方法实现一下。

```scala
case class MinMaxTemp(id: String, min: Double, max: Double, endTs: Long)

val minMaxTempPerWindow2: DataStream[MinMaxTemp] = sensorData
  .map(r => (r.id, r.temperature, r.temperature))
  .keyBy(_._1)
  .timeWindow(Time.seconds(5))
  .reduce(
    (r1: (String, Double, Double), r2: (String, Double, Double)) => {
      (r1._1, r1._2.min(r2._2), r1._3.max(r2._3))
    },
    new AssignWindowEndProcessFunction
  )

class AssignWindowEndProcessFunction
  extends ProcessWindowFunction[(String, Double, Double),
    MinMaxTemp, String, TimeWindow] {
    override def process(key: String,
                       ctx: Context,
                       minMaxIt: Iterable[(String, Double, Double)],
                       out: Collector[MinMaxTemp]): Unit = {
    val minMax = minMaxIt.head
    val windowEnd = ctx.window.getEnd
    out.collect(MinMaxTemp(key, minMax._2, minMax._3, windowEnd))
  }
}
```

