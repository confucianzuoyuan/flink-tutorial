## 搭建执行环境

编写Flink程序的第一件事情就是搭建执行环境。执行环境决定了程序是运行在单机上还是集群上。在DataStream API中，程序的执行环境是由StreamExecutionEnvironment设置的。在我们的例子中，我们通过调用静态getExecutionEnvironment()方法来获取执行环境。这个方法根据调用方法的上下文，返回一个本地的或者远程的环境。如果这个方法是一个客户端提交到远程集群的代码调用的，那么这个方法将会返回一个远程的执行环境。否则，将返回本地执行环境。

也可以用下面的方法来显式的创建本地或者远程执行环境：

**scala version**

```scala
// create a local stream execution environment
val localEnv = StreamExecutionEnvironment
  .createLocalEnvironment()
// create a remote stream execution environment
val remoteEnv = StreamExecutionEnvironment
  .createRemoteEnvironment(
    "host", // hostname of JobManager
    1234, // port of JobManager process
    "path/to/jarFile.jar"
  ) // JAR file to ship to the JobManager
```

**java version**

```java
StreamExecutionEnvironment localEnv = StreamExecutionEnvironment
  .createLocalEnvironment();

StreamExecutionEnvironment remoteEnv = StreamExecutionEnvironment
  .createRemoteEnvironment(
    "host", // hostname of JobManager
    1234, // port of JobManager process
    "path/to/jarFile.jar"
  ); // JAR file to ship to the JobManager
```

接下来，我们使用`env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)`来将我们程序的时间语义设置为事件时间。执行环境提供了很多配置选项，例如：设置程序的并行度和程序是否开启容错机制。

