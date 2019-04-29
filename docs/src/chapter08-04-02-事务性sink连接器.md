### 事务性sink连接器

事务写入sink连接器需要和Flink的检查点机制集成，因为只有在检查点成功完成以后，事务写入sink连接器才会向外部系统commit数据。

为了简化事务性sink的实现，Flink提供了两个模版用来实现自定义sink运算符。这两个模版都实现了CheckpointListener接口。CheckpointListener接口将会从作业管理器接收到检查点完成的通知。

* GenericWriteAheadSink模版会收集检查点之前的所有的数据，并将数据存储到sink任务的运算符状态中。状态保存到了检查点中，并在任务故障的情况下恢复。当任务接收到检查点完成的通知时，任务会将所有的数据写入到外部系统中。
* TwoPhaseCommitSinkFunction模版利用了外部系统的事务特性。对于每一个检查点，任务首先开始一个新的事务，并将接下来所有的数据都写到外部系统的当前事务上下文中去。当任务接收到检查点完成的通知时，sink连接器将会commit这个事务。

*GENERICWRITEAHEADSINK*

GenericWriteAheadSink使得sink运算符可以很方便的实现。这个运算符和Flink的检查点机制集成使用，目标是将每一条数据恰好一次写入到外部系统中去。需要注意的是，在发生故障的情况下，write-ahead log sink可能会不止一次的发送相同的数据。所以GenericWriteAheadSink无法提供完美无缺的恰好处理一次语义的一致性保证，而是仅能提供at-least-once这样的保证。我们接下来详细的讨论这些场景。

GenericWriteAheadSink的原理是将接收到的所有数据都追加到有检查点分割好的预写式日志中去。每当sink运算符碰到检查点屏障，运算符将会开辟一个新的section，并将接下来的所有数据都追加到新的section中去。WAL（预写式日志）将会保存到运算符状态中。由于log能被恢复，所有不会有数据丢失。

当GenericWriteAheadSink接收到检查点完成的通知时，将会发送对应检查点的WAL中存储的所有数据。当所有数据发送成功，对应的检查点必须在内部提交。

检查点的提交分两步。第一步，sink持久化检查点被提交的信息。第二步，删除WAL中所有的数据。我们不能将commit信息保存在Flink应用程序状态中，因为状态不是持久化的，会在故障恢复时重置状态。相反，GenericWriteAheadSink依赖于可插拔的组件在一个外部持久化存储中存储和查找提交信息。这个组件就是CheckpointCommitter。

继承GenericWriteAheadSink的运算符需要提供三个构造器函数。

* CheckpointCommitter
* TypeSerializer，用来序列化输入数据。
* 一个job ID，传给CheckpointCommitter，当应用重启时可以识别commit信息。

还有，write-ahead运算符需要实现一个单独的方法：

```scala
boolean sendValues(Iterable<IN> values, long chkpntId, long timestamp)
```

当检查点完成时，GenericWriteAheadSink调用sendValues()方法来将数据写入到外部存储系统中。这个方法接收一个检查点对应的所有数据的迭代器，检查点的ID，检查点被处理时的时间戳。当数据写入成功时，方法必须返回true，写入失败返回false。

下面的例子展示了如何实现一个写入到标准输出的write-ahead sink。它使用了FileCheckpointCommitter。

```scala
val readings: DataStream[SensorReading] = ...

// write the sensor readings to the standard out via a write-ahead log
readings.transform(
  "WriteAheadSink", new SocketWriteAheadSink)

class StdOutWriteAheadSink extends GenericWriteAheadSink[SensorReading](
    // CheckpointCommitter that commits
    // checkpoints to the local filesystem
    new FileCheckpointCommitter(System.getProperty("java.io.tmpdir")),
    // Serializer for records
    createTypeInformation[SensorReading]
      .createSerializer(new ExecutionConfig),
    // Random JobID used by the CheckpointCommitter
    UUID.randomUUID.toString) {

  override def sendValues(
      readings: Iterable[SensorReading],
      checkpointId: Long,
      timestamp: Long): Boolean = {

    for (r <- readings.asScala) {
      // write record to standard out
      println(r)
    }
    true
  }
}
```

之前我们讲过，GenericWriteAheadSink无法提供完美的exactly-once保证。有两个故障状况会导致数据可能被发送不止一次。

* 当任务执行sendValues()方法时，程序挂掉了。如果外部系统无法原子性的写入所有数据（要么都写入要么都不写），一些数据可能会写入，而另一些数据并没有被写入。由于checkpoint还没有commit，所以在任务恢复的过程中一些数据可能会被再次写入。
* 所有数据都写入成功了，sendValues()方法也返回true了；但在CheckpointCommitter方法被调用之前程序挂了，或者CheckpointCommitter在commit检查点时失败了。那么在恢复的过程中，所有未被提交的检查点将会被重新写入。

*TWOPHASECOMMITSINKFUNCTION*

Flink提供了TwoPhaseCommitSinkFunction接口来简化sink函数的实现。这个接口保证了端到端的exactly-once语义。2PC sink函数是否提供这样的一致性保证取决于我们的实现细节。我们需要讨论一个问题：“2PC协议是否开销太大？”

通常来讲，为了保证分布式系统的一致性，2PC是一个非常昂贵的方法。尽管如此，在Flink的语境下，2PC协议针对每一个检查点只运行一次。TwoPhaseCommitSinkFunction和WAL sink很相似，不同点在于前者不会将数据收集到state中，而是会写入到外部系统事务的上下文中。

TwoPhaseCommitSinkFunction实现了以下协议。在sink任务发送出第一条数据之前，任务将在外部系统中开始一个事务，所有接下来的数据将被写入这个事务的上下文中。当作业管理器初始化检查点并将检查点屏障插入到流中的时候，2PC协议的投票阶段开始。当运算符接收到检查点屏障，运算符将保存它的状态，当保存完成时，运算符将发送一个acknowledgement信息给作业管理器。当sink任务接收到检查点屏障时，运算符将会持久化它的状态，并准备提交当前的事务，以及acknowledge JobManager中的检查点。发送给作业管理器的acknowledgement信息类似于2PC协议中的commit投票。sink任务还不能提交事务，因为它还没有保证所有的任务都已经完成了它们的检查点操作。sink任务也会为下一个检查点屏障之前的所有数据开始一个新的事务。

当作业管理器成功接收到所有任务实例发出的检查点操作成功的通知时，作业管理器将会把检查点完成的通知发送给所有感兴趣的任务。这里的通知对应于2PC协议的提交命令。当sink任务接收到通知时，它将commit所有处于开启状态的事务。一旦sink任务acknowledge了检查点操作，它必须能够commit对应的事务，即使任务发生故障。如果commit失败，数据将会丢失。

让我们总结一下外部系统需要满足什么样的要求：

* 外部系统必须提供事务支持，或者sink的实现能在外部系统上模拟事务功能。
* 在检查点操作期间，事务必须处于open状态，并接收这段时间数据的持续写入。
* 事务必须等到检查点操作完成的通知到来才可以提交。在恢复周期中，可能需要一段时间等待。如果sink系统关闭了事务（例如超时了），那么未被commit的数据将会丢失。
* sink必须在进程挂掉后能够恢复事务。一些sink系统会提供事务ID，用来commit或者abort一个开始的事务。
* commit一个事务必须是一个幂等性操作。sink系统或者外部系统能够观察到事务已经被提交，或者重复提交并没有副作用。

下面的例子可能会让上面的一些概念好理解一些。

```scala
class TransactionalFileSink(val targetPath: String, val tempPath: String)
    extends TwoPhaseCommitSinkFunction[(String, Double), String, Void](
      createTypeInformation[String].createSerializer(new ExecutionConfig),
      createTypeInformation[Void].createSerializer(new ExecutionConfig)) {

  var transactionWriter: BufferedWriter = _

  // Creates a temporary file for a transaction into
  // which the records are written.
  override def beginTransaction(): String = {
    // path of transaction file
    // is built from current time and task index
    val timeNow = LocalDateTime.now(ZoneId.of("UTC"))
      .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
    val taskIdx = this.getRuntimeContext.getIndexOfThisSubtask
    val transactionFile = s"$timeNow-$taskIdx"

    // create transaction file and writer
    val tFilePath = Paths.get(s"$tempPath/$transactionFile")
    Files.createFile(tFilePath)
    this.transactionWriter = Files.newBufferedWriter(tFilePath)
    println(s"Creating Transaction File: $tFilePath")
    // name of transaction file is returned to
    // later identify the transaction
    transactionFile
  }

  /** Write record into the current transaction file. */
  override def invoke(
      transaction: String,
      value: (String, Double),
      context: Context[_]): Unit = {
    transactionWriter.write(value.toString)
    transactionWriter.write('\n')
  }

  /** Flush and close the current transaction file. */
  override def preCommit(transaction: String): Unit = {
    transactionWriter.flush()
    transactionWriter.close()
  }

  // Commit a transaction by moving
  // the precommitted transaction file
  // to the target directory.
  override def commit(transaction: String): Unit = {
    val tFilePath = Paths.get(s"$tempPath/$transaction")
    // check if the file exists to ensure
    // that the commit is idempotent
    if (Files.exists(tFilePath)) {
      val cFilePath = Paths.get(s"$targetPath/$transaction")
      Files.move(tFilePath, cFilePath)
    }
  }

  // Aborts a transaction by deleting the transaction file.
  override def abort(transaction: String): Unit = {
    val tFilePath = Paths.get(s"$tempPath/$transaction")
    if (Files.exists(tFilePath)) {
      Files.delete(tFilePath)
    }
  }
}
```

TwoPhaseCommitSinkFunction[IN, TXN, CONTEXT]包含如下三个范型参数：

* IN表示输入数据的类型。
* TXN定义了一个事务的标识符，可以用来识别和恢复事务。
* CONTEXT定义了自定义的上下文。

TwoPhaseCommitSinkFunction的构造器需要两个TypeSerializer。一个是TXN的类型，另一个是CONTEXT的类型。

最后，TwoPhaseCommitSinkFunction定义了五个需要实现的方法：

* beginTransaction(): TXN开始一个事务，并返回事务的标识符。
* invoke(txn: TXN, value: IN, context: Context[_]): Unit将值写入到当前事务中。
* preCommit(txn: TXN): Unit预提交一个事务。一个预提交的事务不会接收新的写入。
* commit(txn: TXN): Unit提交一个事务。这个操作必须是幂等的。
* abort(txn: TXN): Unit终止一个事务。

