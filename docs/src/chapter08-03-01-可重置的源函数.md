### 可重置的源函数

之前我们讲过，应用程序只有使用可以重播输出数据的数据源时，才能提供令人满意的一致性保证。如果外部系统暴露了获取和重置读偏移量的API，那么source函数就可以重播源数据。这样的例子包括一些能够提供文件流的偏移量的文件系统，或者提供seek方法用来移动到文件的特定位置的文件系统。或者Apache Kafka这种可以为每一个主题的分区提供偏移量并且可以设置分区的读位置的系统。一个反例就是source连接器连接的是socket，socket将会立即丢弃已经发送过的数据。

支持重播输出的源函数需要和Flink的检查点机制集成起来，还需要在检查点被处理时，持久化当前所有的读取位置。当应用从一个保存点（savepoint）恢复或者从故障恢复时，Flink会从最近一次的检查点或者保存点中获取读偏移量。如果程序开始时并不存在状态，那么读偏移量将会被设置到一个默认值。一个可重置的源函数需要实现CheckpointedFunction接口，还需要能够存储读偏移量和相关的元数据，例如文件的路径，分区的ID。这些数据将被保存在list state或者union list state中。

下面的例子将CountSource重写为可重置的数据源。

**scala version**

```scala
class ResettableCountSource
    extends SourceFunction[Long] with CheckpointedFunction {

  var isRunning: Boolean = true
  var cnt: Long = _
  var offsetState: ListState[Long] = _

  override def run(ctx: SourceFunction.SourceContext[Long]) = {
    while (isRunning && cnt < Long.MaxValue) {
      // synchronize data emission and checkpoints
      ctx.getCheckpointLock.synchronized {
        cnt += 1
        ctx.collect(cnt)
      }
    }
  }

  override def cancel() = isRunning = false

  override def snapshotState(
    snapshotCtx: FunctionSnapshotContext
  ): Unit = {
    // remove previous cnt
    offsetState.clear()
    // add current cnt
    offsetState.add(cnt)
  }

  override def initializeState(
      initCtx: FunctionInitializationContext): Unit = {

    val desc = new ListStateDescriptor[Long](
      "offset", classOf[Long])
    offsetState = initCtx
      .getOperatorStateStore
      .getListState(desc)
    // initialize cnt variable
    val it = offsetState.get()
    cnt = if (null == it || !it.iterator().hasNext) {
      -1L
    } else {
      it.iterator().next()
    }
  }
}
```

