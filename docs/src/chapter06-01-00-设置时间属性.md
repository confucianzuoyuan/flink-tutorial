## 设置时间属性

如果我们想要在分布式流处理应用程序中定义有关时间的操作，彻底理解时间的语义是非常重要的。当我们指定了一个窗口去收集某1分钟内的数据时，这个长度为1分钟的桶中，到底应该包含哪些数据？在DataStream API中，我们将使用时间属性来告诉Flink：当我们创建窗口时，我们如何定义时间。时间属性是`StreamExecutionEnvironment`的一个属性，有以下值：

*ProcessingTime*

>机器时间在分布式系统中又叫做“墙上时钟”。

当操作符执行时，此操作符看到的时间是操作符所在机器的机器时间。Processing-time window的触发取决于机器时间，窗口包含的元素也是那个机器时间段内到达的元素。通常情况下，窗口操作符使用processing time会导致不确定的结果，因为基于机器时间的窗口中收集的元素取决于元素到达的速度快慢。使用processing time会为程序提供极低的延迟，因为无需等待水位线的到达。

>如果要追求极限的低延迟，请使用processing time。

*EventTime*

当操作符执行时，操作符看的当前时间是由流中元素所携带的信息决定的。流中的每一个元素都必须包含时间戳信息。而系统的逻辑时钟由水位线(Watermark)定义。我们之前学习过，时间戳要么在事件进入流处理程序之前已经存在，要么就需要在程序的数据源（source）处进行分配。当水位线宣布特定时间段的数据都已经到达，事件时间窗口将会被触发计算。即使数据到达的顺序是乱序的，事件时间窗口的计算结果也将是确定性的。窗口的计算结果并不取决于元素到达的快与慢。

>当水位线超过事件时间窗口的结束时间时，窗口将会闭合，不再接收数据，并触发计算。

*IngestionTime*

当事件进入source操作符时，source操作符所在机器的机器时间，就是此事件的“摄入时间”（IngestionTime），并同时产生水位线。IngestionTime相当于EventTime和ProcessingTime的混合体。一个事件的IngestionTime其实就是它进入流处理器中的时间。

>IngestionTime没什么价值，既有EventTime的执行效率（比较低），有没有EventTime计算结果的准确性。

下面的例子展示了如何设置事件时间。

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment;
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
DataStream<SensorReading> sensorData = env.addSource(...);
```

如果要使用processing time，将`TimeCharacteristic.EventTime`替换为`TimeCharacteristic.ProcessingTIme`就可以了。

