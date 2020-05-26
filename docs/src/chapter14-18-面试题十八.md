## 面试题十八

* Flink上有多少个指标，一个指标一个jar包吗？Flink亲自负责的有几个jar包产出？
* flink的开发中用了哪些算子？
* flink的异步join有了解吗？就是例如kafka和MySQL的流进行join
* flink的broadcast join的原理是什么？
* flink的双流join你们用的时候是类似数据中的left join还是inner join，双流join中怎么确定左表还是右表【没太懂，好像应该是full join，全外连接】，CoProcessFunction是全外连接，基于间隔的Join和基于窗口的Join是Inner Join
* flink集群有多大，怎么部署的？
* hadoop集群有多大，分给flink有多少资源，多少cpu，多少内存，多少slot？
* 你自己写的哪些jar包，用了多少cpu，用了内存，多少个slot？
* 有没有关注你的jar包的处理性能，就是处理kafka的qps和tps？
* 你们有用过flink的背压吗，怎么做优化还是调整？
* flink的知识点还有啥想介绍的？

