## Sink

Flink没有类似于spark中foreach方法，让用户进行迭代的操作。所有对外的输出操作都要利用Sink完成。最后通过类似如下方式完成整个任务最终输出操作。

```java
stream.addSink(new MySink(xxxx));
```

官方提供了一部分的框架的sink。除此以外，需要用户自定义实现sink。

