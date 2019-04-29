### 将hdfs配置为状态后端

首先在IDEA的pom文件中添加依赖：

```xml
		<dependency>
			<groupId>org.apache.hadoop</groupId>
			<artifactId>hadoop-client</artifactId>
			<version>2.8.3</version>
<!--			<scope>provided</scope>-->
		</dependency>
```

在`hdfs-site.xml`添加:

```xml
    <property>
        <name>dfs.permissions</name>
        <value>false</value>
    </property>
```

>别忘了重启hdfs文件系统！

然后添加本地文件夹和hdfs文件的映射：

```sh
hdfs getconf -confKey fs.default.name
hdfs dfs -put /home/parallels/flink/checkpoint hdfs://localhost:9000/flink
```

然后在代码中添加：

```java
env.enableCheckpointing(5000)
env.setStateBackend(new FsStateBackend("hdfs://localhost:9000/flink"))
```

检查一下检查点正确保存了没有：

```sh
hdfs dfs -ls hdfs://localhost:9000/flink
```

