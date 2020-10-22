### 基本转换算子

基本转换算子会针对流中的每一个单独的事件做处理，也就是说每一个输入数据会产生一个输出数据。单值转换，数据的分割，数据的过滤，都是基本转换操作的典型例子。我们将解释这些算子的语义并提供示例代码。

*MAP*

`map`算子通过调用`DataStream.map()`来指定。`map`算子的使用将会产生一条新的数据流。它会将每一个输入的事件传送到一个用户自定义的mapper，这个mapper只返回一个输出事件，这个输出事件和输入事件的类型可能不一样。图5-1展示了一个map算子，这个map将每一个正方形转化成了圆形。

![](images/spaf_0501.png)

`MapFunction`的类型与输入事件和输出事件的类型相关，可以通过实现`MapFunction`接口来定义。接口包含`map()`函数，这个函数将一个输入事件恰好转换为一个输出事件。

```
// T: the type of input elements
// O: the type of output elements
MapFunction[T, O]
    > map(T): O
```

下面的代码实现了将`Event`中的`key`字段抽取出来的功能。

```java
DataStream<Event> stream = env.addSource(new EventSource()); 
DataStream<String> keys = stream.map(new KeyExtractor());

public static class IdExtractor implements MapFunction<Event, String> {
    @Override
    public String map(Event e) throws Exception {
        return e.key;
    }
}
```

当然我们更推荐匿名函数的写法。

```java
DataStream<String> keys = stream.map(e -> e.key);
```

如果要使用匿名函数将流中的数据map成元组，那么需要使用`.returns`方法明确告诉编译器元组的类型信息，因为Java没有元组类型，而这里的元组类型是Flink为Java提供的。例如：

```java
stream
    .map(e -> Tuple2.of(e.key, e.value))
    .returns(new TypeHint<Tuple2<String, Long>>() {});
```

*FILTER*

`filter`转换算子通过在每个输入事件上对一个布尔条件进行求值来过滤掉一些元素，然后将剩下的元素继续发送。一个`true`的求值结果将会把输入事件保留下来并发送到输出，而如果求值结果为`false`，则输入事件会被抛弃掉。我们通过调用`DataStream.filter()`来指定流的`filter`算子，`filter`操作将产生一条新的流，其类型和输入流中的事件类型是一样的。图5-2展示了只产生白色方框的`filter`操作。

![](images/spaf_0502.png)

布尔条件可以使用函数、FilterFunction接口或者匿名函数来实现。FilterFunction中的泛型是输入事件的类型。定义的`filter()`方法会作用在每一个输入元素上面，并返回一个布尔值。

```
// T: the type of elements
FilterFunction[T]
    > filter(T): Boolean
```

下面的例子展示了如何使用filter来从传感器数据中过滤掉温度值小于25华氏温度的读数。

```java
DataStream<Event> filteredStream = stream.filter(e -> e.value >= 100L);
```

*FLATMAP*

`flatMap`算子和`map`算子很类似，不同之处在于针对每一个输入事件`flatMap`可以生成0个、1个或者多个输出元素。事实上，`flatMap`转换算子是`filter`和`map`的泛化。所以`flatMap`可以实现`map`和`filter`算子的功能。图5-3展示了`flatMap`如何根据输入事件的颜色来做不同的处理。如果输入事件是白色方框，则直接输出。输入元素是黑框，则复制输入。灰色方框会被过滤掉。

![](images/spaf_0503.png)

flatMap算子将会应用在每一个输入事件上面。对应的`FlatMapFunction`定义了`flatMap()`方法，这个方法返回0个、1个或者多个事件到一个`Collector`集合中，作为输出结果。

```
// T: the type of input elements
// O: the type of output elements
FlatMapFunction[T, O]
    > flatMap(T, Collector[O]): Unit
```

下面的例子展示了在数据分析教程中经常用到的例子，我们用`flatMap`来实现。使用`_`来切割key，比如`key_1`。

```java
public static class KeySplitter implements FlatMapFunction<String, String> {
    @Override
    public void flatMap(String key, Collector<String> out) {

        String[] splits = key.split("_");

        for (String split : splits) {
            out.collect(split);
        }
    }
}
```

匿名函数写法：

```java
DataStream<String> splitKeys = stream
        .flatMap((FlatMapFunction<String, String>)
                (key, out) -> { for (String s: key.split("_")) { out.collect(s);}})
        // provide result type because Java cannot infer return type of lambda function
        // 提供结果的类型，因为Java无法推断匿名函数的返回值类型
        .returns(Types.STRING);
```
