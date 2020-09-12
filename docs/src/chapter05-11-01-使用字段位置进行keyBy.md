### 使用字段位置进行keyBy

```java
DataStream<Tuple3<Int, String, Long>> input = ...
KeyedStream<Tuple3<Int, String, Long>, String> keyed = input.keyBy(1);
```

如果我们想要用元组的第2个字段和第3个字段做keyBy，可以看下面的例子。

```java
input.keyBy(1, 2);
```

