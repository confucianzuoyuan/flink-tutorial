### 创建表环境

表环境（TableEnvironment）是flink中集成Table API & SQL的核心概念。它负责:

* 在内部的 catalog 中注册 Table
* 注册外部的 catalog
* 加载可插拔模块
* 执行 SQL 查询
* 注册自定义函数 （scalar、table 或 aggregation）
* 将 DataStream 或 DataSet 转换成 Table
* 持有对 ExecutionEnvironment 或 StreamExecutionEnvironment 的引用

在创建TableEnv的时候，可以多传入一个EnvironmentSettings或者TableConfig参数，可以用来配置TableEnvironment的一些特性。

Table 总是与特定的 TableEnvironment 绑定。不能在同一条查询中使用不同 TableEnvironment 中的表，例如，对它们进行 join 或 union 操作。

TableEnvironment 可以通过静态方法 BatchTableEnvironment.create() 或者 StreamTableEnvironment.create() 在 StreamExecutionEnvironment 或者 ExecutionEnvironment 中创建，TableConfig 是可选项。TableConfig可用于配置TableEnvironment或定制的查询优化和转换过程(参见 查询优化)。

请确保选择与你的编程语言匹配的特定的计划器BatchTableEnvironment/StreamTableEnvironment。

如果两种计划器的 jar 包都在 classpath 中（默认行为），你应该明确地设置要在当前程序中使用的计划器。

基于blink版本的流处理环境（Blink-Streaming-Query）：

```java
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

val bsEnv = StreamExecutionEnvironment.getExecutionEnvironment
val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
val bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings)
```

>这里只提供了 blink planner 的流处理设置。有关 old planner 的批处理和流处理的设置，以及 blink planner 的批处理的设置，请查阅官方文档。
