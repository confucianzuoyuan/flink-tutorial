### 文件系统source连接器

Apache Flink针对文件系统实现了一个可重置的source连接器，将文件看作流来读取数据。如下面的例子所示：

```java
val lineReader = new TextInputFormat(null) 

val lineStream: DataStream[String] = env.readFile[String](
  lineReader,                 // The FileInputFormat
  "hdfs:///path/to/my/data",  // The path to read
  FileProcessingMode
    .PROCESS_CONTINUOUSLY,    // The processing mode
  30000L)                     // The monitoring interval in ms
```

StreamExecutionEnvironment.readFile()接收如下参数：

* FileInputFormat参数，负责读取文件中的内容。
* 文件路径。如果文件路径指向单个文件，那么将会读取这个文件。如果路径指向一个文件夹，FileInputFormat将会扫描文件夹中所有的文件。
* PROCESS_CONTINUOUSLY将会周期性的扫描文件，以便扫描到文件新的改变。
* 30000L表示多久扫描一次监听的文件。

FileInputFormat是一个特定的InputFormat，用来从文件系统中读取文件。FileInputFormat分两步读取文件。首先扫描文件系统的路径，然后为所有匹配到的文件创建所谓的input splits。一个input split将会定义文件上的一个范围，一般通过读取的开始偏移量和读取长度来定义。在将一个大的文件分割成一堆小的splits以后，这些splits可以分发到不同的读任务，这样就可以并行的读取文件了。FileInputFormat的第二步会接收一个input split，读取被split定义的文件范围，然后返回对应的数据。

DataStream应用中使用的FileInputFormat需要实现CheckpointableInputFormat接口。这个接口定义了方法来做检查点和重置文件片段的当前的读取位置。

在Flink 1.7中，Flink提供了一些类，这些类继承了FileInputFormat，并实现了CheckpointableInputFormat接口。TextInputFormat一行一行的读取文件，而CsvInputFormat使用逗号分隔符来读取文件。

