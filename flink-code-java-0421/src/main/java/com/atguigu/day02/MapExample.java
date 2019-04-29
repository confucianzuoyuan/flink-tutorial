package com.atguigu.day02;

import com.atguigu.day02.util.SensorReading;
import com.atguigu.day02.util.SensorSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MapExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());

        stream
                .map(r -> r.id)
                .print();

        stream
                .map(new MapFunction<SensorReading, String>() {
                    @Override
                    public String map(SensorReading sensorReading) throws Exception {
                        return sensorReading.id;
                    }
                })
                .print();

        stream
                .map(new IdExtractor())
                .print();

        env.execute();
    }

    public static class IdExtractor implements MapFunction<SensorReading, String> {
        @Override
        public String map(SensorReading sensorReading) throws Exception {
            return sensorReading.id;
        }
    }
}
