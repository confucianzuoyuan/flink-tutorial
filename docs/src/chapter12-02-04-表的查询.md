### 表的查询

利用外部系统的连接器connector，我们可以读写数据，并在环境的Catalog中注册表。接下来就可以对表做查询转换了。

Flink给我们提供了两种查询方式：Table API和 SQL。

#### Table API的调用

Table API是集成在Scala和Java语言内的查询API。与SQL不同，Table API的查询不会用字符串表示，而是在宿主语言中一步一步调用完成的。

Table API基于代表一张“表”的Table类，并提供一整套操作处理的方法API。这些方法会返回一个新的Table对象，这个对象就表示对输入表应用转换操作的结果。有些关系型转换操作，可以由多个方法调用组成，构成链式调用结构。例如table.select(…).filter(…)，其中select（…）表示选择表中指定的字段，filter(…)表示筛选条件。

代码中的实现如下：

```scala
val sensorTable: Table = tableEnv.from("inputTable")

val resultTable: Table = senorTable
  .select("id, temperature")
  .filter("id ='sensor_1'")
```

#### SQL查询

Flink的SQL集成，基于的是Apache Calcite，它实现了SQL标准。在Flink中，用常规字符串来定义SQL查询语句。SQL 查询的结果，是一个新的 Table。

代码实现如下：

```scala
val resultSqlTable: Table = tableEnv
  .sqlQuery("select id, temperature from inputTable where id ='sensor_1'")
```

或者：

```scala
val resultSqlTable: Table = tableEnv.sqlQuery(
  """
    |select id, temperature
    |from inputTable
    |where id = 'sensor_1'
  """.stripMargin)
```

当然，也可以加上聚合操作，比如我们统计每个sensor温度数据出现的个数，做个count统计：

```scala
val aggResultTable = sensorTable
  .groupBy('id)
  .select('id, 'id.count as 'count)
```

SQL的实现：

```scala
val aggResultSqlTable = tableEnv
  .sqlQuery("select id, count(id) as cnt from inputTable group by id")
```

这里Table API里指定的字段，前面加了一个单引号`'`，这是Table API中定义的Expression类型的写法，可以很方便地表示一个表中的字段。

字段可以直接全部用双引号引起来，也可以用半边单引号+字段名的方式。以后的代码中，一般都用后一种形式。

