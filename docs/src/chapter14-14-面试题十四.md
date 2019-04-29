## 面试题十四

问题：flink消费kakfa保证数据不丢失（flink消费kafka数据不丢不重，flink消费kafka的时候挂了怎么恢复数据）

解答：端到端一致性（exactly-once），flink会维护消费kafka的偏移量，checkpoint操作。

