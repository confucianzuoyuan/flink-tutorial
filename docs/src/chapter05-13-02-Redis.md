### Redis

```xml
<dependency>
  <groupId>org.apache.bahir</groupId>
  <artifactId>flink-connector-redis_2.11</artifactId>
  <version>1.0</version>
</dependency>
```

定义一个redis的mapper类，用于定义保存到redis时调用的命令：

```java
public class WriteToRedis {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment;
    env.setParallelism(1);

    DataStream<SensorReading> stream = env.addSource(new SensorSource());

    FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("localhost").build();

    stream.addSink(new RedisSink<SensorReading>(conf, new MyRedisMapper()));

    env.execute();
  }

  public static class MyRedisMapper implements RedisMapper<SensorReading> {
    // 使用id作为key
    @Override
    public String getKeyFromData(SensorReading t) {
      return t.id;
    }

    // 使用温度作为value
    @Override
    public String getValueFromData(SensorReading t) {
      return t.temperature.toString();
    }

    @Override
    public RedisCommandDescription getCommandDescription() {
      return new RedisCommandDescription(RedisCommand.HSET, "sensor");
    }
  }
}
```

