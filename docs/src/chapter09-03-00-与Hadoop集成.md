## 与Hadoop集成

推荐两种方法

1. 下载包含hadoop的Flink版本。
2. 使用我们之前下载的Flink，然后配置Hadoop的环境变量。 `export HADOOP_CLASSPATH={hadoop classpath}`

我们还需要提供Hadoop配置文件的路径。只需设置名为`HADOOP_CONF_DIR`的环境变量就可以了。这样Flink就能够连上YARN的ResourceManager和HDFS了。

