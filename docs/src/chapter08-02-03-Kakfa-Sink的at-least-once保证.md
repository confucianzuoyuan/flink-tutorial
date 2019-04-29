### Kakfa Sink的at-least-once保证

Flink的Kafka sink提供了基于配置的一致性保证。Kafka sink使用下面的条件提供了至少处理一次保证：

* Flink检查点机制开启，所有的数据源都是可重置的。
* 当写入失败时，sink连接器将会抛出异常，使得应用程序挂掉然后重启。这是默认行为。应用程序内部的Kafka客户端还可以配置为重试写入，只要提前声明当写入失败时，重试几次这样的属性（retries property）。
* sink连接器在完成它的检查点之前会等待Kafka发送已经将数据写入的通知。

