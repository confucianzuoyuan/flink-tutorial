## 面试题五

问题：如果下级存储不支持事务，Flink怎么保证exactly-once？

解答：幂等性写入(Redis, ElasticSearch)。

