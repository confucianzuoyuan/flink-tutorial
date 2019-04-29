### 使用ListCheckpointed接口来实现操作符的列表状态

操作符状态会在操作符的每一个并行实例中去维护。一个操作符并行实例上的所有事件都可以访问同一个状态。Flink支持三种操作符状态：list state, list union state, broadcast state。

一个函数可以实现ListCheckpointed接口来处理操作符的list state。ListCheckpointed接口无法处理ValueState和ListState，因为这些状态是注册在状态后端的。操作符状态类似于成员变量，和状态后端的交互通过ListCheckpointed接口的回调函数实现。接口提供了两个方法：

```java
// 返回函数状态的快照，返回值为列表
snapshotState(checkpointId: Long, timestamp: Long): java.util.List[T]
// 从列表恢复函数状态
restoreState(java.util.List[T] state): Unit
```

当Flink触发stateful functon的一次checkpoint时，snapshotState()方法会被调用。方法接收两个参数，checkpointId为唯一的单调递增的检查点Id，timestamp为当master机器开始做检查点操作时的墙上时钟（机器时间）。方法必须返回序列化好的状态对象的列表。

当宕机程序从检查点或者保存点恢复时会调用restoreState()方法。restoreState使用snapshotState保存的列表来恢复。

下面的例子展示了如何实现ListCheckpointed接口。业务场景为：一个对每一个并行实例的超过阈值的温度的计数程序。

```scala
class HighTempCounter(val threshold: Double)
    extends RichFlatMapFunction[SensorReading, (Int, Long)]
    with ListCheckpointed[java.lang.Long] {

  // index of the subtask
  private lazy val subtaskIdx = getRuntimeContext
    .getIndexOfThisSubtask
  // local count variable
  private var highTempCnt = 0L

  override def flatMap(
      in: SensorReading,
      out: Collector[(Int, Long)]): Unit = {
    if (in.temperature > threshold) {
      // increment counter if threshold is exceeded
      highTempCnt += 1
      // emit update with subtask index and counter
      out.collect((subtaskIdx, highTempCnt))
    }
  }

  override def restoreState(
      state: util.List[java.lang.Long]): Unit = {
    highTempCnt = 0
    // restore state by adding all longs of the list
    for (cnt <- state.asScala) {
      highTempCnt += cnt
    }
  }

  override def snapshotState(
      chkpntId: Long,
      ts: Long): java.util.List[java.lang.Long] = {
    // snapshot state as list with a single count
    java.util.Collections.singletonList(highTempCnt)
  }
}
```

上面的例子中，每一个并行实例都计数了本实例有多少温度值超过了设定的阈值。例子中使用了操作符状态，并且每一个并行实例都拥有自己的状态变量，这个状态变量将会被检查点操作保存下来，并且可以通过使用ListCheckpointed接口来恢复状态变量。

看了上面的例子，我们可能会有疑问，那就是为什么操作符状态是状态对象的列表。这是因为列表数据结构支持包含操作符状态的函数的并行度改变的操作。为了增加或者减少包含了操作符状态的函数的并行度，操作符状态需要被重新分区到更多或者更少的并行任务实例中去。而这样的操作需要合并或者分割状态对象。而对于每一个有状态的函数，分割和合并状态对象都是很常见的操作，所以这显然不是任何类型的状态都能自动完成的。

通过提供一个状态对象的列表，拥有操作符状态的函数可以使用snapshotState()方法和restoreState()方法来实现以上所说的逻辑。snapshotState()方法将操作符状态分割成多个部分，restoreState()方法从所有的部分中将状态对象收集起来。当函数的操作符状态恢复时，状态变量将被分区到函数的所有不同的并行实例中去，并作为参数传递给restoreState()方法。如果并行任务的数量大于状态对象的数量，那么一些并行任务在开始的时候是没有状态的，所以restoreState()函数的参数为空列表。

再来看一下上面的程序，我们可以看到操作符的每一个并行实例都暴露了一个状态对象的列表。如果我们增加操作符的并行度，那么一些并行任务将会从0开始计数。为了获得更好的状态分区的行为，当HighTempCounter函数扩容时，我们可以按照下面的程序来实现snapshotState()方法，这样就可以把计数值分配到不同的并行计数中去了。

```scala
override def snapshotState(
    chkpntId: Long,
    ts: Long): java.util.List[java.lang.Long] = {
  // split count into ten partial counts
  val div = highTempCnt / 10
  val mod = (highTempCnt % 10).toInt
  // return count as ten parts
  (List.fill(mod)(new java.lang.Long(div + 1)) ++
    List.fill(10 - mod)(new java.lang.Long(div))).asJava
}
```

