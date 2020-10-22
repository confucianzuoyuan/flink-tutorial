## 转换算子的使用

一旦我们有一条DataStream，我们就可以在这条数据流上面使用转换算子了。转换算子有很多种。一些转换算子可以产生一条新的DataStream，当然这个DataStream的类型可能是新类型。还有一些转换算子不会改变原有DataStream的数据，但会将数据流分区或者分组。业务逻辑就是由转换算子串起来组合而成的。

在我们的例子中，我们首先使用`map()`转换算子将事件的时间戳ETL掉。然后，我们使用`keyBy()`转换算子将流按照`key`进行分区。接下来，我们定义了一个`timeWindow()`转换算子，这个算子将每个`key`所对应的分区的点击事件分配到了5秒钟的滚动窗口中。

**java version**

```java
DataStream<Tuple3<String, Long, Long>> accumulated = eventStream
  .map(r -> Tuple2.of(r.key, r.value))
  .returns(new TypeHint<Tuple2<String, Long>>() {})
  .keyBy(r -> r.key)
  .timeWindow(Time.seconds(5))
  .process(new AvgOfValue());
```

窗口转换算子将在“窗口操作符”一章中讲解。最后，我们使用了一个UDF函数来计算每个`key`在每个窗口的`value`的平均值。我们稍后将会讨论UDF函数的实现。

