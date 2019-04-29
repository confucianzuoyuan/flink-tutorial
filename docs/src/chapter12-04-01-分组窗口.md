### 分组窗口

分组窗口（Group Windows）会根据时间或行计数间隔，将行聚合到有限的组（Group）中，并对每个组的数据执行一次聚合函数。

Table API中的Group Windows都是使用.window（w:GroupWindow）子句定义的，并且必须由as子句指定一个别名。为了按窗口对表进行分组，窗口的别名必须在group by子句中，像常规的分组字段一样引用。

```scala
val table = input
  .window([w: GroupWindow] as $"w") // 定义窗口，别名 w
  .groupBy($"w", $"a")  // 以属性a和窗口w作为分组的key
  .select($"a", $"b".sum)  // 聚合字段b的值，求和
```

或者，还可以把窗口的相关信息，作为字段添加到结果表中：

```scala
val table = input
  .window([w: GroupWindow] as $"w")
  .groupBy($"w", $"a")
  .select($"a", $"w".start, $"w".end, $"w".rowtime, $"b".count)
```

Table API提供了一组具有特定语义的预定义Window类，这些类会被转换为底层DataStream或DataSet的窗口操作。

Table API支持的窗口定义，和我们熟悉的一样，主要也是三种：滚动（Tumbling）、滑动（Sliding）和会话（Session）。

#### 滚动窗口

滚动窗口（Tumbling windows）要用Tumble类来定义，另外还有三个方法：

* over：定义窗口长度
* on：用来分组（按时间间隔）或者排序（按行数）的时间字段
* as：别名，必须出现在后面的groupBy中

代码如下：

```scala
// Tumbling Event-time Window（事件时间字段rowtime
.window(Tumble over 10.minutes on $"rowtime" as $"w")
// Tumbling Processing-time Window（处理时间字段proctime）
.window(Tumble over 10.minutes on $"proctime" as $"w")
// Tumbling Row-count Window (类似于计数窗口，按处理时间排序，10行一组)
.window(Tumble over 10.rows on $"proctime" as $"w")
```

#### 滑动窗口

滑动窗口（Sliding windows）要用Slide类来定义，另外还有四个方法：

* over：定义窗口长度
* every：定义滑动步长
* on：用来分组（按时间间隔）或者排序（按行数）的时间字段
* as：别名，必须出现在后面的groupBy中

代码如下：

```scala
// Sliding Event-time Window
.window(Slide over 10.minutes every 5.minutes on $"rowtime" as $"w")
// Sliding Processing-time window
.window(Slide over 10.minutes every 5.minutes on $"proctime" as $"w")
// Sliding Row-count window
.window(Slide over 10.rows every 5.rows on $"proctime" as $"w")
```

#### 会话窗口

会话窗口（Session windows）要用Session类来定义，另外还有三个方法：

* withGap：会话时间间隔
* on：用来分组（按时间间隔）或者排序（按行数）的时间字段
* as：别名，必须出现在后面的groupBy中

代码如下：

```scala
// Session Event-time Window
.window(Session withGap 10.minutes on $"rowtime" as $"w")
// Session Processing-time Window
.window(Session withGap 10.minutes on $"proctime" as $"w")
```

