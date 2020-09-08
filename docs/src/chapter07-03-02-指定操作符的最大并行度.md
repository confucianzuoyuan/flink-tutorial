### 指定操作符的最大并行度

操作符的最大并行度定义了操作符的keyed state可以被分到多少个key groups中。

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment;

// set the maximum parallelism for this application
env.setMaxParallelism(512);

DataStream<Tuple3<String, Double, Double>> alerts = keyedSensorData
  .flatMap(new TemperatureAlertFunction(1.1))
  // set the maximum parallelism for this operator and
  // override the application-wide value
  .setMaxParallelism(1024);
```

