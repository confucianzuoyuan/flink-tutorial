### 将表转换成DataStream

表可以转换为DataStream或DataSet。这样，自定义流处理或批处理程序就可以继续在 Table API或SQL查询的结果上运行了。

将表转换为DataStream或DataSet时，需要指定生成的数据类型，即要将表的每一行转换成的数据类型。通常，最方便的转换类型就是Row。当然，因为结果的所有字段类型都是明确的，我们也经常会用元组类型来表示。

表作为流式查询的结果，是动态更新的。所以，将这种动态查询转换成的数据流，同样需要对表的更新操作进行编码，进而有不同的转换模式。

Table API中表到DataStream有两种模式：

* 追加模式（Append Mode）

用于表只会被插入（Insert）操作更改的场景。

* 撤回模式（Retract Mode）

用于任何场景。有些类似于更新模式中Retract模式，它只有Insert和Delete两类操作。

得到的数据会增加一个Boolean类型的标识位（返回的第一个字段），用它来表示到底是新增的数据（Insert），还是被删除的数据（老数据，Delete）。

代码实现如下：

```java
val resultStream: DataStream[Row] = tableEnv
  .toAppendStream[Row](resultTable)

val aggResultStream: DataStream[(Boolean, (String, Long))] = tableEnv
  .toRetractStream[(String, Long)](aggResultTable)

resultStream.print("result")
aggResultStream.print("aggResult")
```

所以，没有经过groupby之类聚合操作，可以直接用toAppendStream来转换；而如果经过了聚合，有更新操作，一般就必须用toRetractDstream。

