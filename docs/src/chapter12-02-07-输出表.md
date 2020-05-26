### 输出表

表的输出，是通过将数据写入 TableSink 来实现的。TableSink 是一个通用接口，可以支持不同的文件格式、存储数据库和消息队列。

具体实现，输出表最直接的方法，就是通过 Table.insertInto() 方法将一个 Table 写入注册过的 TableSink 中。

#### 输出到文件

代码如下：

```scala
// 注册输出表
tableEnv.connect(
    new FileSystem().path("…\\resources\\out.txt")
  ) // 定义到文件系统的连接
  .withFormat(new Csv()) // 定义格式化方法，Csv格式
  .withSchema(
    new Schema()
      .field("id", DataTypes.STRING())
      .field("temp", DataTypes.DOUBLE())
  ) // 定义表结构
  .createTemporaryTable("outputTable") // 创建临时表

resultSqlTable.insertInto("outputTable")
```

#### 更新模式（Update Mode）

在流处理过程中，表的处理并不像传统定义的那样简单。

对于流式查询（Streaming Queries），需要声明如何在（动态）表和外部连接器之间执行转换。与外部系统交换的消息类型，由更新模式（update mode）指定。

Flink Table API中的更新模式有以下三种：

1. 追加模式（Append Mode）

在追加模式下，表（动态表）和外部连接器只交换插入（Insert）消息。

2. 撤回模式（Retract Mode）

在撤回模式下，表和外部连接器交换的是：添加（Add）和撤回（Retract）消息。

* 插入（Insert）会被编码为添加消息；
* 删除（Delete）则编码为撤回消息；
* 更新（Update）则会编码为，已更新行（上一行）的撤回消息，和更新行（新行）的添加消息。

在此模式下，不能定义key，这一点跟upsert模式完全不同。

3. Upsert（更新插入）模式

在Upsert模式下，动态表和外部连接器交换Upsert和Delete消息。

这个模式需要一个唯一的key，通过这个key可以传递更新消息。为了正确应用消息，外部连接器需要知道这个唯一key的属性。

* 插入（Insert）和更新（Update）都被编码为Upsert消息；
* 删除（Delete）编码为Delete信息。

这种模式和Retract模式的主要区别在于，Update操作是用单个消息编码的，所以效率会更高。

#### 输出到Kafka

除了输出到文件，也可以输出到Kafka。我们可以结合前面Kafka作为输入数据，构建数据管道，kafka进，kafka出。

代码如下：

```scala
// 输出到 kafka
tableEnv.connect(
  new Kafka()
    .version("0.11")
    .topic("sinkTest")
    .property("zookeeper.connect", "localhost:2181")
    .property("bootstrap.servers", "localhost:9092")
  )
  .withFormat(new Csv())
  .withSchema(
    new Schema()
      .field("id", DataTypes.STRING())
      .field("temp", DataTypes.DOUBLE())
  )
  .createTemporaryTable("kafkaOutputTable")
  
resultTable.insertInto("kafkaOutputTable")
```

#### 输出到ElasticSearch

ElasticSearch的connector可以在upsert（update+insert，更新插入）模式下操作，这样就可以使用Query定义的键（key）与外部系统交换UPSERT/DELETE消息。

另外，对于“仅追加”（append-only）的查询，connector还可以在append 模式下操作，这样就可以与外部系统只交换insert消息。

es目前支持的数据格式，只有Json，而flink本身并没有对应的支持，所以还需要引入依赖：

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-json</artifactId>
  <version>${flink.version}</version>
</dependency>
```

代码实现如下：

```scala
// 输出到es
tableEnv.connect(
  new Elasticsearch()
    .version("6")
    .host("localhost", 9200, "http")
    .index("sensor")
    .documentType("temp")
  )
  .inUpsertMode()           // 指定是 Upsert 模式
  .withFormat(new Json())
  .withSchema(
    new Schema()
      .field("id", DataTypes.STRING())
      .field("count", DataTypes.BIGINT())
  )
  .createTemporaryTable("esOutputTable")
  
aggResultTable.insertInto("esOutputTable")
```

#### 输出到MySql

Flink专门为Table API的jdbc连接提供了flink-jdbc连接器，我们需要先引入依赖：

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-jdbc_2.11</artifactId>
  <version>${flink.version}</version>
</dependency>
```

jdbc连接的代码实现比较特殊，因为没有对应的java/scala类实现ConnectorDescriptor，所以不能直接tableEnv.connect()。不过Flink SQL留下了执行DDL的接口：tableEnv.sqlUpdate()。

对于jdbc的创建表操作，天生就适合直接写DDL来实现，所以我们的代码可以这样写：

```scala
// 输出到 Mysql
val sinkDDL: String =
  """
    |create table jdbcOutputTable (
    |  id varchar(20) not null,
    |  cnt bigint not null
    |) with (
    |  'connector.type' = 'jdbc',
    |  'connector.url' = 'jdbc:mysql://localhost:3306/test',
    |  'connector.table' = 'sensor_count',
    |  'connector.driver' = 'com.mysql.jdbc.Driver',
    |  'connector.username' = 'root',
    |  'connector.password' = '123456'
    |)
  """.stripMargin

tableEnv.sqlUpdate(sinkDDL)
aggResultSqlTable.insertInto("jdbcOutputTable")
```

