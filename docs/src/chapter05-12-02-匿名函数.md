### 匿名函数

匿名函数可以实现一些简单的逻辑，但无法实现一些高级功能，例如访问状态等等。

```java
DataStream<String> tweets = ...
DataStream<String> flinkTweets = tweets.filter(r -> r.contains("flink"));
```