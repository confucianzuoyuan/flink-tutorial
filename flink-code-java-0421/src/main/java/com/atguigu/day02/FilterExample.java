package com.atguigu.day02;

import com.atguigu.day02.util.SensorReading;
import com.atguigu.day02.util.SensorSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FilterExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
        System.out.println(env.getParallelism());

        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource()).setParallelism(1);

        stream
                .filter(r -> r.id.equals("sensor_1"));

        stream
                .filter(new FilterFunction<SensorReading>() {
                    @Override
                    public boolean filter(SensorReading sensorReading) throws Exception {
                        return sensorReading.id.equals("sensor_1");
                    }
                });

        stream
                .filter(new MyFilter())
                .print();

        env.execute();
    }

    public static class MyFilter implements FilterFunction<SensorReading> {
        @Override
        public boolean filter(SensorReading sensorReading) throws Exception {
            return sensorReading.id.equals("sensor_1");
        }
    }
}
