## 下载Flink运行时环境，提交Jar包的运行方式

下载链接：http://mirror.bit.edu.cn/apache/flink/flink-1.11.1/flink-1.11.1-bin-scala_2.11.tgz

然后解压

```sh
$ tar xvfz flink-1.11.1-bin-scala_2.11.tgz
```

启动Flink集群

```sh
$ cd flink-1.11.1
$ ./bin/start-cluster.sh
```

可以打开Flink WebUI查看集群状态：http://localhost:8081

在`IDEA`中使用`maven package`打包。

提交打包好的`JAR`包

```sh
$ cd flink-1.11.1
$ ./bin/flink run 打包好的JAR包的绝对路径
```

停止Flink集群

```sh
$ ./bin/stop-cluster.sh
```

查看标准输出日志的位置，在`log`文件夹中。

```sh
$ cd flink-1.11.1/log
```

