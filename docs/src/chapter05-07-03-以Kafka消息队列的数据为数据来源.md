### 以Kafka消息队列的数据为数据来源

**scala version**

```scala
val properties = new Properties()
properties.setProperty("bootstrap.servers", "localhost:9092")
properties.setProperty("group.id", "consumer-group")
properties.setProperty(
  "key.deserializer",
  "org.apache.kafka.common.serialization.StringDeserializer"
)
properties.setProperty(
  "value.deserializer",
  "org.apache.kafka.common.serialization.StringDeserializer"
)
properties.setProperty("auto.offset.reset", "latest")
val env = StreamExecutionEnvironment.getExecutionEnvironment
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
env.setParallelism(1)
val stream = env
  // source为来自Kafka的数据，这里我们实例化一个消费者，topic为hotitems
  .addSource(
    new FlinkKafkaConsumer011[String](
      "hotitems",
      new SimpleStringSchema(),
      properties
    )
  )
```

**java version**

```java
Properties properties = new Properties();
properties.setProperty("bootstrap.servers", "localhost:9092");
properties.setProperty("group.id", "consumer-group");
properties.setProperty(
  "key.deserializer",
  "org.apache.kafka.common.serialization.StringDeserializer"
);
properties.setProperty(
  "value.deserializer",
  "org.apache.kafka.common.serialization.StringDeserializer"
);
properties.setProperty("auto.offset.reset", "latest");
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment;
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
env.setParallelism(1);
DataStream<String> stream = env
  // source为来自Kafka的数据，这里我们实例化一个消费者，topic为hotitems
  .addSource(
    new FlinkKafkaConsumer011<String>(
      "hotitems",
      new SimpleStringSchema(),
      properties
    )
  );
```
