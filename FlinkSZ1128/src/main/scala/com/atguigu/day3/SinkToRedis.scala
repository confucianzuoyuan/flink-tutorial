package com.atguigu.day3

import com.atguigu.day2.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object SinkToRedis {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)

    // redis的主机
    val conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build()
    stream.addSink(new RedisSink[SensorReading](conf, new MyRedisMapper))
    env.execute()
  }

  class MyRedisMapper extends RedisMapper[SensorReading] {
    // 要使用的redis命令
    override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(RedisCommand.HSET, "sensor")
    }

    // 哈希表中的key是什么
    override def getKeyFromData(t: SensorReading): String = t.id

    // 哈希表中的value是什么
    override def getValueFromData(t: SensorReading): String = t.temperature.toString
  }
}