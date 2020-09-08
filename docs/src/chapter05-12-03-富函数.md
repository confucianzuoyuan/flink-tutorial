### 富函数

我们经常会有这样的需求：在函数处理数据之前，需要做一些初始化的工作；或者需要在处理数据时可以获得函数执行上下文的一些信息；以及在处理完数据时做一些清理工作。而DataStream API就提供了这样的机制。

DataStream API提供的所有转换操作函数，都拥有它们的“富”版本，并且我们在使用常规函数或者匿名函数的地方来使用富函数。例如下面就是富函数的一些例子，可以看出，只需要在常规函数的前面加上`Rich`前缀就是富函数了。

* RichMapFunction
* RichFlatMapFunction
* RichFilterFunction
* ...

当我们使用富函数时，我们可以实现两个额外的方法：

* open()方法是rich function的初始化方法，当一个算子例如map或者filter被调用之前open()会被调用。open()函数通常用来做一些只需要做一次即可的初始化工作。
* close()方法是生命周期中的最后一个调用的方法，通常用来做一些清理工作。

另外，getRuntimeContext()方法提供了函数的RuntimeContext的一些信息，例如函数执行的并行度，当前子任务的索引，当前子任务的名字。同时还它还包含了访问**分区状态**的方法。下面看一个例子：

```java
public static class MyFlatMap extends RichFlatMapFunction<Integer, Tuple2<Integer, Integer>> {
  private int subTaskIndex = 0;

  @Override
  public void open(Configuration configuration) {
    int subTaskIndex = getRuntimeContext.getIndexOfThisSubtask;
    // 做一些初始化工作
    // 例如建立一个和HDFS的连接
  }

  @Override
  public void flatMap(Integer in, Collector<Tuple2<Integer, Integer>> out) {
    if (in % 2 == subTaskIndex) {
      out.collect((subTaskIndex, in));
    }
  }

  @Override
  public void close() {
    // 清理工作，断开和HDFS的连接。
  }
}
```

