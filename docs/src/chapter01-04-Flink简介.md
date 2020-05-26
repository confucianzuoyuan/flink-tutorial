## Flink简介

Apache Flink是第三代分布式流处理器，它拥有极富竞争力的功能。它提供准确的大规模流处理，具有高吞吐量和低延迟。特别的是，以下功能使Flink脱颖而出：

* 事件时间（event-time）和处理时间（processing-tme）语义。即使对于无序事件流，事件时间（event-time）语义仍然能提供一致且准确的结果。而处理时间（processing-time）语义可用于具有极低延迟要求的应用程序。
* 精确一次（exactly-once）的状态一致性保证。
* 每秒处理数百万个事件，毫秒级延迟。 Flink应用程序可以扩展为在数千个核（cores）上运行。
* 分层API，具有不同的权衡表现力和易用性。本书介绍了DataStream API和过程函数（process function），为常见的流处理操作提供原语，如窗口和异步操作，以及精确控制状态和时间的接口。本书不讨论Flink的关系API，SQL和LINQ风格的Table API。
* 连接到最常用的存储系统，如Apache Kafka，Apache Cassandra，Elasticsearch，JDBC，Kinesis和（分布式）文件系统，如HDFS和S3。
* 由于其高可用的设置（无单点故障），以及与Kubernetes，YARN和Apache Mesos的紧密集成，再加上从故障中快速恢复和动态扩展任务的能力，Flink能够以极少的停机时间7*24全天候运行流应用程序。
* 能够更新应用程序代码并将作业（jobs）迁移到不同的Flink集群，而不会丢失应用程序的状态。
* 详细且可自定义的系统和应用程序指标集合，以提前识别问题并对其做出反应。
* 最后但同样重要的是，Flink也是一个成熟的批处理器。

除了这些功能之外，Flink还是一个非常易于开发的框架，因为它易于使用的API。嵌入式执行模式，可以在单个JVM进程中启动应用程序和整个Flink系统，这种模式一般用于在IDE中运行和调试Flink作业。在开发和测试Flink应用程序时，此功能非常有用。

