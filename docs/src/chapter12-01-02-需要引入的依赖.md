### 需要引入的依赖

取决于你使用的编程语言，比如这里，我们选择 Scala API 来构建你的 Table API 和 SQL 程序：

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-api-scala-bridge_2.11</artifactId>
  <version>1.11.0</version>
  <scope>provided</scope>
</dependency>
```

除此之外，如果你想在 IDE 本地运行你的程序，你需要添加下面的模块，具体用哪个取决于你使用哪个 Planner，我们这里选择使用 blink planner：

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-planner-blink_2.11</artifactId>
  <version>1.11.0</version>
  <scope>provided</scope>
</dependency>
```

如果你想实现自定义格式来解析 Kafka 数据，或者自定义函数，使用下面的依赖：

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-common</artifactId>
  <version>1.11.0</version>
  <scope>provided</scope>
</dependency>
```

* flink-table-planner-blink：planner计划器，是table API最主要的部分，提供了运行时环境和生成程序执行计划的planner；
* flink-table-api-scala-bridge：bridge桥接器，主要负责table API和 DataStream/DataSet API的连接支持，按照语言分java和scala。

这里的两个依赖，是IDE环境下运行需要添加的；如果是生产环境，lib目录下默认已经有了planner，就只需要有bridge就可以了。

>需要注意的是：flink table本身有两个 planner 计划器，在flink 1.11之后，已经默认使用 blink planner，如果想了解 old planner，可以查阅官方文档。

