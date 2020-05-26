### Kafka Sink的恰好处理一次语义保证

Kafka 0.11版本引入了事务写特性。由于这个新特性，Flink Kafka sink可以为输出结果提供恰好处理一次语义的一致性保证，只要经过合适的配置就行。Flink程序必须开启检查点机制，并从可重置的数据源进行消费。FlinkKafkaProducer还提供了包含Semantic参数的构造器来控制sink提供的一致性保证。可能的取值如下：

* Semantic.NONE，不提供任何一致性保证。数据可能丢失或者被重写多次。
* Semantic.AT_LEAST_ONCE，保证无数据丢失，但可能被处理多次。这个是默认设置。
* Semantic.EXACTLY_ONCE，基于Kafka的事务性写入特性实现，保证每条数据恰好处理一次。

