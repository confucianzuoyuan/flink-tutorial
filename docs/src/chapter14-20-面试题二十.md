## 面试题二十

你们flink输出的目标数据库是什么，答看需求到es或者mysql需要自定义mysqlsink，他问自定义mysql sink里面实际上是jdbc做的？你们有没有发现用jdbc并发的写mysql他的性能很差，怎么处理的？答：一般不直接写入mysql，一般先写入消息队列（redis，kafka，rabbitmq，...），用消息队列来保护mysql。
