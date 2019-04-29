## 面试题一

问题：公司怎么提交的实时任务，有多少Job Manager？

解答：

* 我们每次提交都会创建一个新的Flink集群，为每个job提供一个yarn-session，任务之间互相独立，互不影响，方便管理。任务执行完成之后创建的集群也会消失。(作业模式)

线上脚本如下：

```sh
$ bin/yarn-session.sh -n 7 -s 8 -jm 3072 -tm 32768 -qu root.*.*-nm *-* -d
```

其中申请7个taskManager，每个8核，每个taskmanager有32768M内存。

* 集群默认只有一个Job Manager。但为了防止单点故障，我们配置了高可用。我们公司一般配置一个主Job Manager，两个备用Job Manager，然后结合ZooKeeper的使用，来达到高可用。

>作为参考，快手的Flink集群的机器数量是1500台。

