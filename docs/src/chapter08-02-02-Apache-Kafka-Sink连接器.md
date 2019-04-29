### Apache Kafka Sink连接器

添加依赖：

```xml
<dependency>
   <groupId>org.apache.flink</groupId>
   <artifactId>flink-connector-kafka_2.12</artifactId>
   <version>1.7.1</version>
</dependency>
```

下面的例子展示了如何创建一个Kafka sink

```scala
val stream: DataStream[String] = ...

val myProducer = new FlinkKafkaProducer[String](
  "localhost:9092",         // broker list
  "topic",                  // target topic
  new SimpleStringSchema)   // serialization schema

stream.addSink(myProducer)
```

