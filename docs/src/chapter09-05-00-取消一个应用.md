## 取消一个应用

```sh
$ ./bin/flink cancel <jobId>
```

取消的同时做保存点操作

```sh
$ ./bin/flink cancel -s [savepointPath] <jobId>
```

例如

```sh
$ ./bin/flink cancel -s \
hdfs:///xxx:50070/savepoints d5fdaff43022954f5f02fcd8f25ef855
Cancelling job bc0b2ad61ecd4a615d92ce25390f61ad 
with savepoint to hdfs:///xxx:50070/savepoints.
Cancelled job bc0b2ad61ecd4a615d92ce25390f61ad. 
Savepoint stored in hdfs:///xxx:50070/savepoints/savepoint-bc0b2a-d08de07fbb10.
```

