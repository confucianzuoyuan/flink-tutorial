### Kafka

Kafka版本为0.11

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-kafka-0.11_2.11</artifactId>
  <version>${flink.version}</version>
</dependency>
```

Kafka版本为2.0以上

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-kafka_2.11</artifactId>
  <version>${flink.version}</version>
</dependency>
```

主函数中添加sink：

```java
DataStream<String> union = high
  .union(low)
  .map(r -> r.temperature.toString);

union.addSink(
  new FlinkKafkaProducer011<String>(
    "localhost:9092",
    "test",
    new SimpleStringSchema()
  )
);
```
