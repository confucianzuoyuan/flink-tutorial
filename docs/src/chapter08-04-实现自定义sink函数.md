## 实现自定义sink函数

DataStream API中，任何运算符或者函数都可以向外部系统发送数据。DataStream不需要最终流向sink运算符。例如，我们可能实现了一个FlatMapFunction，这个函数将每一个接收到的数据通过HTTP POST请求发送出去，而不使用Collector发送到下一个运算符。DataStream API也提供了SinkFunction接口以及对应的rich版本RichSinkFunction抽象类。SinkFunction接口提供了一个方法：

```scala
void invode(IN value, Context ctx)
```

SinkFunction的Context可以访问当前处理时间，当前水位线，以及数据的时间戳。

下面的例子展示了一个简单的SinkFunction，可以将传感器读数写入到socket中去。需要注意的是，我们需要在启动Flink程序前启动一个监听相关端口的进程。否则将会抛出ConnectException异常。可以运行“nc -l localhost 9191”命令。

```scala
val readings: DataStream[SensorReading] = ...

// write the sensor readings to a socket
readings.addSink(new SimpleSocketSink("localhost", 9191))
  // set parallelism to 1 because only one thread can write to a socket
  .setParallelism(1)

// -----

class SimpleSocketSink(val host: String, val port: Int)
    extends RichSinkFunction[SensorReading] {

  var socket: Socket = _
  var writer: PrintStream = _

  override def open(config: Configuration): Unit = {
    // open socket and writer
    socket = new Socket(InetAddress.getByName(host), port)
    writer = new PrintStream(socket.getOutputStream)
  }

  override def invoke(
      value: SensorReading,
      ctx: SinkFunction.Context[_]): Unit = {
    // write sensor reading to socket
    writer.println(value.toString)
    writer.flush()
  }

  override def close(): Unit = {
    // close writer and socket
    writer.close()
    socket.close()
  }
}
```

之前我们讨论过，端到端的一致性保证建立在sink连接器的属性上面。为了达到端到端的恰好处理一次语义的目的，应用程序需要幂等性的sink连接器或者事务性的sink连接器。上面例子中的SinkFunction既不是幂等写入也不是事务性的写入。由于socket具有只能添加（append-only）这样的属性，所以不可能实现幂等性的写入。又因为socket不具备内置的事务支持，所以事务性写入就只能使用Flink的WAL sink特性来实现了。接下来我们将学习如何实现幂等sink连接器和事务sink连接器。

