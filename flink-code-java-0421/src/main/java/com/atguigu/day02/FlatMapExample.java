package com.atguigu.day02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlatMapExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> stream = env.fromElements("white", "black", "gray");

        stream
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String s, Collector<String> collector) throws Exception {
                        if (s.equals("white")) {
                            collector.collect(s);
                        } else if (s.equals("black")) {
                            collector.collect(s);
                            collector.collect(s);
                        }
                    }
                })
                .print();

        stream
                .flatMap(new MyFlatMap())
                .print();

        env.execute();
    }

    public static class MyFlatMap implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String s, Collector<String> collector) throws Exception {
            if (s.equals("white")) {
                collector.collect(s);
            } else if (s.equals("black")) {
                collector.collect(s);
                collector.collect(s);
            }
        }
    }
}
