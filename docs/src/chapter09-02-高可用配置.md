## 高可用配置

Flink的高可用配置需要Apache ZooKeeper组件，以及一个分布式文件系统，例如HDFS等等。作业管理器将会把相关信息都存储在文件系统中，并将指向文件系统中相关信息的指针保存在ZooKeeper中。一旦失败，一个新的作业管理器将从ZooKeeper中指向相关信息的指针所指向的文件系统中读取元数据，并恢复运行。

配置文件编写

```yaml
high-availability.zookeeper.quorum: address1:2181[,...],addressX:2181
high-availability.storageDir: hdfs:///flink/recovery
high-availability.zookeeper.path.root: /flink
```

