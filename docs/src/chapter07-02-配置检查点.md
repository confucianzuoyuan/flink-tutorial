## 配置检查点

10秒钟保存一次检查点。

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment;

// set checkpointing interval to 10 seconds (10000 milliseconds)
env.enableCheckpointing(10000L);
```

