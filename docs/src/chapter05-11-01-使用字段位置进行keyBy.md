### 使用字段位置进行keyBy

```scala
val input: DataStream[(Int, String, Long)] = ...
val keyed = input.keyBy(1)
```

>注意，要么明确写清楚类型注释，要么让Scala去做类型推断，不要用IDEA的类型推断功能。

如果我们想要用元组的第2个字段和第3个字段做keyBy，可以看下面的例子。

```scala
val keyed2 = input.keyBy(1, 2)
```

