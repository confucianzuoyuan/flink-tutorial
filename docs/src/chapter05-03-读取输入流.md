## 读取输入流

一旦执行环境设置好，就该写业务逻辑了。`StreamExecutionEnvironment`提供了创建数据源的方法，这些方法可以从数据流中将数据摄取到程序中。数据流可以来自消息队列或者文件系统，也可能是实时产生的（例如socket）。

在我们的例子里面，我们这样写：

**scala version**

```scala
val sensorData: DataStream[SensorReading] = env
  .addSource(new SensorSource)
```

**java version**

```java
DataStream<SensorReading> sensorData = env
  .addSource(new SensorSource());
```

这样就可以连接到传感器测量数据的数据源并创建一个类型为`SensorReading`的`DataStream`了。Flink支持很多数据类型，我们将在接下来的章节里面讲解。在我们的例子里面，我们的数据类型是一个定义好的Scala样例类。`SensorReading`样例类包含了传感器ID，数据的测量时间戳，以及测量温度值。`assignTimestampsAndWatermarks(new SensorTimeAssigner)`方法指定了如何设置事件时间语义的时间戳和水位线。有关`SensorTimeAssigner`我们后面再讲。

