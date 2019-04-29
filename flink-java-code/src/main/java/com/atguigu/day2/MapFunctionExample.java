package com.atguigu.day2;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MapFunctionExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<SensorReading> stream = env.addSource(new SensorSource());

        stream.map(r -> r.id);

        stream.map(new MapFunction<SensorReading, String>() {
            @Override
            public String map(SensorReading value) throws Exception {
                return value.id;
            }
        });

        stream.map(new MyFlatMapFunction()).print();

        env.execute();
    }

    public static class MyFlatMapFunction implements MapFunction<SensorReading, String> {
        @Override
        public String map(SensorReading value) throws Exception {
            return value.id;
        }
    }
}
