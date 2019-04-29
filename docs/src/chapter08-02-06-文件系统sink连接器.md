### 文件系统sink连接器

在将流处理应用配置成exactly-once检查点机制，以及配置成所有源数据都能在故障的情况下可以重置，Flink的StreamingFileSink提供了端到端的恰好处理一次语义保证。下面的例子展示了StreamingFileSink的使用方式。

```java
val input: DataStream[String] = …
val sink: StreamingFileSink[String] = StreamingFileSink
  .forRowFormat(
    new Path("/base/path"), 
    new SimpleStringEncoder[String]("UTF-8"))
  .build()

input.addSink(sink)
```

当StreamingFileSink接到一条数据，这条数据将被分配到一个桶（bucket）中。一个桶是我们配置的“/base/path”的子目录。

Flink使用BucketAssigner来分配桶。BucketAssigner是一个公共的接口，为每一条数据返回一个BucketId，BucketId决定了数据被分配到哪个子目录。如果没有指定BucketAssigner，Flink将使用DateTimeBucketAssigner来将每条数据分配到每个一个小时所产生的桶中去，基于数据写入的处理时间（机器时间，墙上时钟）。

StreamingFileSink提供了exactly-once输出的保证。sink通过一个commit协议来达到恰好处理一次语义的保证。这个commit协议会将文件移动到不同的阶段，有以下状态：in progress，pending，finished。这个协议基于Flink的检查点机制。当Flink决定roll a file时，这个文件将被关闭并移动到pending状态，通过重命名文件来实现。当下一个检查点完成时，pending文件将被移动到finished状态，同样是通过重命名来实现。

一旦任务故障，sink任务需要将处于in progress状态的文件重置到上一次检查点的写偏移量。这个可以通过关闭当前in progress的文件，并将文件结尾无效的部分丢弃掉来实现。

