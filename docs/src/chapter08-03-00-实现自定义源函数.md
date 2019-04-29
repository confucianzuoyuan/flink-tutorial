## 实现自定义源函数

DataStream API提供了两个接口来实现source连接器：

* SourceFunction和RichSourceFunction可以用来定义非并行的source连接器，source跑在单任务上。
* ParallelSourceFunction和RichParallelSourceFunction可以用来定义跑在并行实例上的source连接器。

除了并行于非并行的区别，这两种接口完全一样。就像process function的rich版本一样，RichSourceFunction和RichParallelSourceFunction的子类可以override open()和close()方法，也可以访问RuntimeContext，RuntimeContext提供了并行任务实例的数量，当前任务实例的索引，以及一些其他信息。

SourceFunction和ParallelSourceFunction定义了两种方法：

* void run(SourceContext<T> ctx)
* cancel()

run()方法用来读取或者接收数据然后将数据摄入到Flink应用中。根据接收数据的系统，数据可能是推送的也可能是拉取的。Flink仅仅在特定的线程调用run()方法一次，通常情况下会是一个无限循环来读取或者接收数据并发送数据。任务可以在某个时间点被显式的取消，或者由于流是有限流，当数据被消费完毕时，任务也会停止。

当应用被取消或者关闭时，cancel()方法会被Flink调用。为了优雅的关闭Flink应用，run()方法需要在cancel()被调用以后，立即终止执行。下面的例子显示了一个简单的源函数的例子：从0数到Long.MaxValue。

```java
class CountSource extends SourceFunction[Long] {
  var isRunning: Boolean = true

  override def run(ctx: SourceFunction.SourceContext[Long]) = {

    var cnt: Long = -1
    while (isRunning && cnt < Long.MaxValue) {
      cnt += 1
      ctx.collect(cnt)
    }
  }

  override def cancel() = isRunning = false
}
```

