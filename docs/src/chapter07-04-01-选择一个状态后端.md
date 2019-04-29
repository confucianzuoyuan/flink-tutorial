### 选择一个状态后端

* MemoryStateBackend将状态当作Java的对象(没有序列化操作)存储在TaskManager JVM进程的堆上。
* FsStateBackend将状态存储在本地的文件系统或者远程的文件系统如HDFS。
* RocksDBStateBackend将状态存储在RocksDB中。

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment;

String checkpointPath = ???
// configure path for checkpoints on the remote filesystem
// env.setStateBackend(new FsStateBackend("file:///tmp/checkpoints"))

val backend = new RocksDBStateBackend(checkpointPath)
// configure the state backend
env.setStateBackend(backend);
```

