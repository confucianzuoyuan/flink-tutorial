### yarn集群高可用配置

首先在yarn集群的配置文件`yarn-site.xml`中加入以下代码

```xml
<property>
  <name>yarn.resourcemanager.am.max-attempts</name>
  <value>4</value>
  <description>
    The maximum number of application master execution attempts.
    Default value is 2, i.e., an application is restarted at most once.
  </description>
</property>
```

然后在`./conf/flink-conf.yaml`加上

```yaml
yarn.application-attempts: 4
```

