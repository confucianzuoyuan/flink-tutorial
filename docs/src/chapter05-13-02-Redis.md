### Redis

```xml
<dependency>
  <groupId>org.apache.bahir</groupId>
  <artifactId>flink-connector-redis_2.11</artifactId>
  <version>1.0</version>
</dependency>
```

定义一个redis的mapper类，用于定义保存到redis时调用的命令：

```scala
object WriteToRedis {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)

    val conf = new FlinkJedisPoolConfig.Builder().setHost("localhost").build()

    stream.addSink(new RedisSink[SensorReading](conf, new MyRedisMapper))

    env.execute()

  }

  class MyRedisMapper extends RedisMapper[SensorReading] {
    // 使用id作为key
    override def getKeyFromData(t: SensorReading): String = t.id

    // 使用温度作为value
    override def getValueFromData(t: SensorReading): String = t.temperature.toString

    override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(RedisCommand.HSET, "sensor")
    }
  }
}
```

