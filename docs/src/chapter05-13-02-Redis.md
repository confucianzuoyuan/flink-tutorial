### Redis

```xml
<dependency>
  <groupId>org.apache.bahir</groupId>
  <artifactId>flink-connector-redis_2.11</artifactId>
  <version>1.0</version>
</dependency>
```

定义一个redis的mapper类，用于定义保存到redis时调用的命令：

**scala version**

```scala
object SinkToRedisExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)

    val conf = new FlinkJedisPoolConfig.Builder().setHost("localhost").build()

    stream.addSink(new RedisSink[SensorReading](conf, new MyRedisSink))

    env.execute()
  }

  class MyRedisSink extends RedisMapper[SensorReading] {
    override def getKeyFromData(t: SensorReading): String = t.id

    override def getValueFromData(t: SensorReading): String = t.temperature.toString

    override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.HSET, "sensor")
  }
}
```

**java version**

```java
public class WriteToRedisExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<SensorReading> stream = env.addSource(new SensorSource());


        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("localhost").build();

        stream.addSink(new RedisSink<SensorReading>(conf, new MyRedisSink()));

        env.execute();
    }

    public static class MyRedisSink implements RedisMapper<SensorReading> {
        @Override
        public String getKeyFromData(SensorReading sensorReading) {
            return sensorReading.id;
        }

        @Override
        public String getValueFromData(SensorReading sensorReading) {
            return sensorReading.temperature + "";
        }

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "sensor");
        }
    }
}
```

