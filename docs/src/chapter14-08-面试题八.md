## 面试题八

问题：Flink的checkpoint机制对比spark有什么不同和优势？

解答：spark streaming的Checkpoint仅仅是针对driver的故障恢复做了数据和元数据的Checkpoint。而flink的checkpoint机制要复杂了很多，它采用的是轻量级的分布式快照，实现了每个操作符的快照，及循环流的在循环的数据的快照。参见教材内容和链接：

```
https://cloud.tencent.com/developer/article/1189624
```

