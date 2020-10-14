### 将代码部署到flink运行时环境

1. 将项目中`pom.xml`的所有依赖全部标注为`provided`
2. 下载以下`jar`包，并存放到`flink-1.11.2/lib`中

```
flink-table-api-java-bridge_2.11-1.11.0.jar
flink-table-common-1.11.0.jar
flink-table-planner-blink_2.11-1.11.0.jar
guava-27.0-jre.jar
hive-exec-3.1.2.jar
libfb303-0.9.3.jar
flink-connector-hive_2.11-1.11.0.jar
```

3. 打包运行程序

```sh
$ ./bin/flink run jar包
```
