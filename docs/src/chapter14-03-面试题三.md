## 面试题三

问题：为什么使用Flink替代Spark？

解答：教材里面有Flink和Spark的详细对比。

一，Flink是真正的流处理，延迟在毫秒级，Spark Streaming是微批，延迟在秒级。

二，Flink可以处理事件时间，而Spark Streaming只能处理机器时间，无法保证时间语义的正确性。

三，Flink的检查点算法比Spark Streaming更加灵活，性能更高。Spark Streaming的检查点算法是在每个stage结束以后，才会保存检查点。

四，Flink易于实现端到端一致性。

