## 读取输入流

一旦执行环境设置好，就该写业务逻辑了。`StreamExecutionEnvironment`提供了创建数据源的方法，这些方法可以从数据流中将数据摄取到程序中。数据流可以来自消息队列或者文件系统，也可能是实时产生的（例如socket）。

在我们的例子里面，我们这样写：

```java
DataStream<Event> stream = env.addSource(new EventSource());
```

**Event POJO Class**

```java
public class Event {
    public String key;
    public Long value;
    public Long timestamp;

    public Event(String key, Long value, Long timestamp) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }

    public Event() {
    }

    @Override
    public String toString() {
        return "Event{" + "key='" + key + '\'' + ", value='" + value + '\'' + ", timestamp=" + timestamp + '}';
    }
}
```

这样就可以连接到传感器测量数据的数据源并创建一个类型为`Event`的`DataStream`了。Flink支持很多数据类型，我们将在接下来的章节里面讲解。在我们的例子里面，我们的数据类型是一个定义好的Java的POJO Class。`Event`类包含了`key`，`value`，以及`时间戳`。

我们这里将流中的数据抽象成了一个三元组（键，值，时间戳），有利于我们抛开业务，抓住流处理的本质。
