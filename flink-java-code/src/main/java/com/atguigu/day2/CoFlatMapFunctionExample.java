package com.atguigu.day2;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

public class CoFlatMapFunctionExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Tuple2<Integer, String>> stream1 = env.fromElements(
                new Tuple2<>(1, "aaaaa"),
                new Tuple2<>(2, "bbbbb")
        );

        DataStream<Tuple2<Integer, String>> stream2 = env.fromElements(
                new Tuple2<>(1, "ccccc"),
                new Tuple2<>(2, "ddddd")
        );

        stream1
                .keyBy(r -> r.f0)
                .connect(stream2.keyBy(r -> r.f0))
                .flatMap(new CoFlatMapFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, String>() {
                    @Override
                    public void flatMap1(Tuple2<Integer, String> value, Collector<String> out) throws Exception {
                        out.collect(value.f1 + " 来自第一条流的元素发送两次");
                        out.collect(value.f1 + " 来自第一条流的元素发送两次");
                    }

                    @Override
                    public void flatMap2(Tuple2<Integer, String> value, Collector<String> out) throws Exception {
                        out.collect(value.f1 + " 来自第二条流的元素发送一次");
                    }
                })
                .print();

        env.execute();
    }
}