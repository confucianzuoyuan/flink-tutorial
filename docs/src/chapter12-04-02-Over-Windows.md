### Over Windows

Over window聚合是标准SQL中已有的（Over子句），可以在查询的SELECT子句中定义。Over window 聚合，会针对每个输入行，计算相邻行范围内的聚合。Over windows使用.window（w:overwindows*）子句定义，并在select()方法中通过别名来引用。

比如这样：

```scala
val table = input
  .window([w: OverWindow] as $"w")
  .select($"a"", $"b".sum over $"w", $"c".min over $"w")
```

Table API提供了Over类，来配置Over窗口的属性。可以在事件时间或处理时间，以及指定为时间间隔、或行计数的范围内，定义Over windows。

无界的over window是使用常量指定的。也就是说，时间间隔要指定UNBOUNDED_RANGE，或者行计数间隔要指定UNBOUNDED_ROW。而有界的over window是用间隔的大小指定的。

实际代码应用如下：

1. 无界的 over window

```scala
// 无界的事件时间over window (时间字段 "rowtime")
.window(Over partitionBy $"a" orderBy $"rowtime" preceding UNBOUNDED_RANGE as $"w")
//无界的处理时间over window (时间字段"proctime")
.window(Over partitionBy $"a" orderBy $"proctime" preceding UNBOUNDED_RANGE as $"w")
// 无界的事件时间Row-count over window (时间字段 "rowtime")
.window(Over partitionBy $"a" orderBy $"rowtime" preceding UNBOUNDED_ROW as $"w")
//无界的处理时间Row-count over window (时间字段 "rowtime")
.window(Over partitionBy $"a" orderBy $"proctime" preceding UNBOUNDED_ROW as $"w")
```

2. 有界的over window

```scala
// 有界的事件时间over window (时间字段 "rowtime"，之前1分钟)
.window(Over partitionBy $"a" orderBy $"rowtime" preceding 1.minutes as $"w")
// 有界的处理时间over window (时间字段 "rowtime"，之前1分钟)
.window(Over partitionBy $"a" orderBy $"proctime" preceding 1.minutes as $"w")
// 有界的事件时间Row-count over window (时间字段 "rowtime"，之前10行)
.window(Over partitionBy $"a" orderBy $"rowtime" preceding 10.rows as $"w")
// 有界的处理时间Row-count over window (时间字段 "rowtime"，之前10行)
.window(Over partitionBy $"a" orderBy $"proctime" preceding 10.rows as $"w")
```

