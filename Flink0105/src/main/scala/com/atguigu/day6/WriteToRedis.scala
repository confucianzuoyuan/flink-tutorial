package com.atguigu.day6

import com.atguigu.day2.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

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