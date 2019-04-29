### 函数类

Flink暴露了所有udf函数的接口(实现方式为接口或者抽象类)。例如MapFunction, FilterFunction, ProcessFunction等等。

例子实现了FilterFunction接口

```java
class FilterFilter extends FilterFunction<String> {
  @Override
  public Boolean filter(String value) {
    return value.contains("flink");
  }
}

DataStream<String> flinkTweets = tweets.filter(new FlinkFilter);
```

还可以将函数实现成匿名类

```java
DataStream<String> flinkTweets = tweets.filter(
  new RichFilterFunction<String> {
    @Override
    public Boolean filter(String value) {
      return value.contains("flink");
    }
  }
)
```

我们filter的字符串"flink"还可以当作参数传进去。

```java
DataStream<String> tweets = ...
DataStream<String> flinkTweets = tweets.filter(new KeywordFilter("flink"));

class KeywordFilter(keyWord: String) extends FilterFunction<String> {
  @Override
  public Boolean filter(String value) = {
    return value.contains(keyWord);
  }
}
```

