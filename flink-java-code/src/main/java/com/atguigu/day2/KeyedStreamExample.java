package com.atguigu.day2;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyedStreamExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<SensorReading> stream = env.addSource(new SensorSource());

        stream
                .map(r -> Tuple2.of(r.id, r.temperature))
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Double>>() { }))
                .filter(r -> r.f0.equals("sensor_1"))
                .keyBy(r -> r.f0)
                .reduce(new ReduceFunction<Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> reduce(Tuple2<String, Double> value1, Tuple2<String, Double> value2) throws Exception {
                        if (value1.f1 > value2.f1) {
                            return value1;
                        } else {
                            return value2;
                        }
                    }
                });

        stream
                .map(r -> new Tuple2<String, Double>(r.id, r.temperature))
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Double>>() { }))
                .filter(r -> r.f0.equals("sensor_1"))
                .keyBy(r -> r.f0)
                .reduce(new MyReduceFunction());

        stream
                .map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> map(SensorReading value) throws Exception {
                        return new Tuple2<>(value.id, value.temperature);
                    }
                })
                .filter(r -> r.f0.equals("sensor_1"))
                .keyBy(r -> r.f0)
                .reduce(new MyReduceFunction())
                .print();

        env.execute();
    }

    public static class MyReduceFunction implements ReduceFunction<Tuple2<String, Double>> {
        @Override
        public Tuple2<String, Double> reduce(Tuple2<String, Double> value1, Tuple2<String, Double> value2) throws Exception {
            if (value1.f1 > value2.f1) {
                return value1;
            } else {
                return value2;
            }
        }
    }
}
