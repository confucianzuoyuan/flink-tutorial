### Maven依赖

主要包含三部分的依赖：flink和hive的连接器，hive的依赖和hadoop的依赖。

```xml
<!-- Flink Dependency -->
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-hive_2.11</artifactId>
  <version>1.11.0</version>
  <scope>provided</scope>
</dependency>

<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-api-java-bridge_2.11</artifactId>
  <version>1.11.0</version>
  <scope>provided</scope>
</dependency>

<!-- Hive Dependency -->
<dependency>
    <groupId>org.apache.hive</groupId>
    <artifactId>hive-exec</artifactId>
    <version>${hive.version}</version>
    <scope>provided</scope>
</dependency>

<dependency>
	<groupId>org.apache.hadoop</groupId>
	<artifactId>hadoop-common</artifactId>
	<version>${hadoop.version}</version>
	<!--            <scope>provided</scope>-->
</dependency>
<dependency>
	<groupId>org.apache.hadoop</groupId>
	<artifactId>hadoop-hdfs</artifactId>
	<version>${hadoop.version}</version>
	<!--<scope>provided</scope>-->
</dependency>

<dependency>
	<groupId>org.apache.hadoop</groupId>
	<artifactId>hadoop-mapreduce-client-core</artifactId>
	<version>${hadoop.version}</version>
</dependency>
```