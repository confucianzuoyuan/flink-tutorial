### 基本程序结构

Table API 和 SQL 的程序结构，与流式处理的程序结构类似；也可以近似地认为有这么几步：首先创建执行环境，然后定义source、transform和sink。

具体操作流程如下：

```java
val tableEnv = ... // 创建表环境

// 创建表
tableEnv.connect(...).createTemporaryTable("table1")
// 注册输出表
tableEnv.connect(...).createTemporaryTable("outputTable")

// 使用 Table API query 创建表
val tapiResult = tableEnv.from("table1").select(...)
// 使用 SQL query 创建表
val sqlResult  = tableEnv.sqlQuery("SELECT ... FROM table1 ...")

// 输出一张结果表到 TableSink，SQL查询的结果表也一样
TableResult tableResult = tapiResult.executeInsert("outputTable");
tableResult...

// 执行
tableEnv.execute("scala_job")
```

