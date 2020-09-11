package com.atguigu.day2;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyedStreamExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<SensorReading> stream = env.addSource(new SensorSource());

        stream
                .filter(r -> r.id.equals("sensor_1"))
                .keyBy(r -> r.id)
                .reduce(new ReduceFunction<SensorReading>() {
                    @Override
                    public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                        if (value1.temperature > value2.temperature) {
                            return value1;
                        } else {
                            return value2;
                        }
                    }
                })
                .print();

        env.execute();
    }
}
