### 在Catalog中注册表

#### 表（Table）的概念

TableEnvironment可以注册目录Catalog，并可以基于Catalog注册表。它会维护一个Catalog-Table表之间的map。

表（Table）是由一个“标识符”来指定的，由3部分组成：Catalog名、数据库（database）名和对象名（表名）。如果没有指定目录或数据库，就使用当前的默认值。

表可以是常规的（Table，表），或者虚拟的（View，视图）。常规表（Table）一般可以用来描述外部数据，比如文件、数据库表或消息队列的数据，也可以直接从 DataStream转换而来。视图可以从现有的表中创建，通常是table API或者SQL查询的一个结果。

#### 连接到文件系统（Csv格式）

连接外部系统在Catalog中注册表，直接调用tableEnv.connect()就可以，里面参数要传入一个ConnectorDescriptor，也就是connector描述器。对于文件系统的connector而言，flink内部已经提供了，就叫做FileSystem()。

代码如下：

```scala
tableEnv
  .connect(new FileSystem().path("sensor.txt"))  // 定义表数据来源，外部连接
  .withFormat(new OldCsv())    // 定义从外部系统读取数据之后的格式化方法
  .withSchema(
    new Schema()
      .field("id", DataTypes.STRING())
      .field("timestamp", DataTypes.BIGINT())
      .field("temperature", DataTypes.DOUBLE())
  )    // 定义表结构
  .createTemporaryTable("inputTable")    // 创建临时表
```

这是旧版本的csv格式描述器。由于它是非标的，跟外部系统对接并不通用，所以将被弃用，以后会被一个符合RFC-4180标准的新format描述器取代。新的描述器就叫Csv()，但flink没有直接提供，需要引入依赖flink-csv：

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-csv</artifactId>
  <version>${flink.version}</version>
</dependency>
```

代码非常类似，只需要把withFormat里的OldCsv改成Csv就可以了。

#### 连接到Kafka

kafka的连接器flink-kafka-connector中，1.11版本的已经提供了Table API的支持。我们可以在 connect方法中直接传入一个叫做Kafka的类，这就是kafka连接器的描述器ConnectorDescriptor。

```scala
tableEnv
  .connect(
    new Kafka()
      .version("0.11") // 定义kafka的版本
      .topic("sensor") // 定义主题
      .property("zookeeper.connect", "localhost:2181")
      .property("bootstrap.servers", "localhost:9092")
  )
  .withFormat(new Csv())
  .withSchema(
    new Schema()
      .field("id", DataTypes.STRING())
      .field("timestamp", DataTypes.BIGINT())
      .field("temperature", DataTypes.DOUBLE())
  )
  .createTemporaryTable("kafkaInputTable")
```

当然也可以连接到ElasticSearch、MySql、HBase、Hive等外部系统，实现方式基本上是类似的。

