### 创建临时视图

创建临时视图的第一种方式，就是直接从DataStream转换而来。同样，可以直接对应字段转换；也可以在转换的时候，指定相应的字段。

代码如下：

```java
tableEnv.createTemporaryView("sensorView", dataStream)
tableEnv.createTemporaryView("sensorView",
  dataStream, $"id", $"temperature", $"timestamp" as "ts")
```

另外，当然还可以基于Table创建视图：

```java
tableEnv.createTemporaryView("sensorView", sensorTable)
```

View和Table的Schema完全相同。事实上，在Table API中，可以认为View和Table是等价的。

