## Flink提供的连接器

Flink提供了读写很多存储系统的连接器。消息队列，日志系统，例如Apache Kafka, Kinesis, RabbitMQ等等这些是常用的数据源。在批处理环境中，数据流很可能是监听一个文件系统，而当新的数据落盘的时候，读取这些新数据。

在sink一端，数据流经常写入到消息队列中，以供接下来的流处理程序消费。数据流也可能写入到文件系统中做持久化，或者交给批处理程序来进行分析。数据流还可能被写入到key-value存储或者关系型数据库中，例如Cassandra，ElasticSearch或者MySQL中，这样数据可供查询，还可以在仪表盘中显示出来。

不幸的是，对于大多数存储系统并没有标准接口，除了针对DBMS的JDBC。相反，每一个存储系统都需要有自己的特定的连接器。所以，Flink需要维护针对不同存储系统（消息队列，日志系统，文件系统，k-v数据库，关系型数据库等等）的连接器实现。

Flink提供了针对Apache Kafka, Kinesis, RabbitMQ, Apache Nifi, 各种文件系统，Cassandra, Elasticsearch, 还有JDBC的连接器。除此之外，Apache Bahir项目还提供了额外的针对例如ActiveMQ, Akka, Flume, Netty, 和Redis等的连接器。

