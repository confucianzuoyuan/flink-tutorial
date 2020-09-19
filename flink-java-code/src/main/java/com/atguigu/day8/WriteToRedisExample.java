package com.atguigu.day8;

import com.atguigu.day2.SensorReading;
import com.atguigu.day2.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

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