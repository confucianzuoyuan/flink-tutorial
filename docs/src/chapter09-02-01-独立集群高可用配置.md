### 独立集群高可用配置

需要在配置文件中加一行集群标识符信息，因为可能多个集群共用一个zookeeper服务。

```yaml
high-availability.cluster-id: /cluster-1
```

