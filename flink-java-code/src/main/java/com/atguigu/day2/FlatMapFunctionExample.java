package com.atguigu.day2;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlatMapFunctionExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> stream = env.fromElements("white", "black", "gray");

        stream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                if (value.equals("white")) {
                    out.collect(value);
                } else if (value.equals("black")) {
                    out.collect(value);
                    out.collect(value);
                }
            }
        });

        stream.flatMap(new MyFlatMapFunction()).print();

        env.execute();
    }

    public static class MyFlatMapFunction implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            if (value.equals("white")) {
                out.collect(value);
            } else if (value.equals("black")) {
                out.collect(value);
                out.collect(value);
            }
        }
    }
}
