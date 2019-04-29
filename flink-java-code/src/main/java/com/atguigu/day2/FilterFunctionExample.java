package com.atguigu.day2;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FilterFunctionExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<SensorReading> stream = env.addSource(new SensorSource());

        stream.filter(r -> r.temperature > 0.0);

        stream.filter(new FilterFunction<SensorReading>() {
            @Override
            public boolean filter(SensorReading value) throws Exception {
                return value.temperature > 0.0;
            }
        });

        stream.filter(new MyFilterFunction()).print();

        env.execute();
    }

    public static class MyFilterFunction implements FilterFunction<SensorReading> {
        @Override
        public boolean filter(SensorReading value) throws Exception {
            return value.temperature > 0.0;
        }
    }
}
