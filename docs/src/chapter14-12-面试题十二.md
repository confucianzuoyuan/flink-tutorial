## 面试题十二

问题：Flink三种时间概念分别说出应用场景？

解答：

1. Event Time：见教材。

2. Processing Time：没有事件时间的情况下，或者对实时性要求超高的情况下。

3. Ingestion Time：存在多个Source Operator的情况下，每个Source Operator会使用自己本地系统时钟指派Ingestion Time。后续基于时间相关的各种操作，都会使用数据记录中的Ingestion Time。

