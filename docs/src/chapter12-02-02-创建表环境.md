### 创建表环境

创建表环境最简单的方式，就是基于流处理执行环境，调create方法直接创建：

```scala
val tableEnv = StreamTableEnvironment.create(env)
```

表环境（TableEnvironment）是flink中集成Table API & SQL的核心概念。它负责:

* 注册catalog
* 在内部 catalog 中注册表
* 执行 SQL 查询
* 注册用户自定义函数
* 将 DataStream 或 DataSet 转换为表
* 保存对 ExecutionEnvironment 或 StreamExecutionEnvironment 的引用

在创建TableEnv的时候，可以多传入一个EnvironmentSettings或者TableConfig参数，可以用来配置TableEnvironment的一些特性。

比如，配置老版本的流式查询（Flink-Streaming-Query）：

```scala
val settings = EnvironmentSettings
  .newInstance()
  .useOldPlanner()      // 使用老版本planner
  .inStreamingMode()    // 流处理模式
  .build()
  
val tableEnv = StreamTableEnvironment.create(env, settings)
```

基于老版本的批处理环境（Flink-Batch-Query）：

```scala
val batchEnv = ExecutionEnvironment.getExecutionEnvironment
val batchTableEnv = BatchTableEnvironment.create(batchEnv)
```

基于blink版本的流处理环境（Blink-Streaming-Query）：

```scala
val bsSettings = EnvironmentSettings
  .newInstance()
  .useBlinkPlanner()
  .inStreamingMode()
  .build()

val bsTableEnv = StreamTableEnvironment.create(env, bsSettings)
```

基于blink版本的批处理环境（Blink-Batch-Query）：

```scala
val bbSettings = EnvironmentSettings
  .newInstance()
  .useBlinkPlanner()
  .inBatchMode().build()
  
val bbTableEnv = TableEnvironment.create(bbSettings)
```

