package com.atguigu.day02;

import com.atguigu.day02.util.SensorReading;
import com.atguigu.day02.util.SensorSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class UnionExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<SensorReading> sensor_1 = env.addSource(new SensorSource()).filter(r -> r.id.equals("sensor_1"));
        SingleOutputStreamOperator<SensorReading> sensor_2 = env.addSource(new SensorSource()).filter(r -> r.id.equals("sensor_2"));
        SingleOutputStreamOperator<SensorReading> sensor_3 = env.addSource(new SensorSource()).filter(r -> r.id.equals("sensor_3"));

        sensor_1.union(sensor_2, sensor_3).print();

        env.execute();
    }
}
