### 定义窗口操作符

Window算子可以在keyed stream或者nokeyed stream上面使用。

创建一个Window算子，需要指定两个部分：

1. `window assigner`定义了流的元素如何分配到window中。window assigner将会产生一条WindowedStream(或者AllWindowedStream，如果是nonkeyed DataStream的话)

2. window function用来处理WindowedStream(AllWindowedStream)中的元素。

下面的代码说明了如何使用窗口操作符。

```scala
stream
  .keyBy(...)
  .window(...)  // 指定window assigner
  .reduce/aggregate/process(...) // 指定window function

stream
  .windowAll(...) // 指定window assigner
  .reduce/aggregate/process(...) // 指定window function
```

>我们的学习重点是Keyed WindowedStream。

