package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountFromSocket {
    public static void main(String[] args) throws Exception {
        // runtime context
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // num of task parallelism
        env.setParallelism(9);

        DataStream<String> stream = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = stream
                // Map
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String[] arr = s.split(" ");
                        for (String word : arr) {
                            collector.collect(Tuple2.of(word, 1));
                        }
                    }
                })
                // shuffle
                .keyBy(r -> r.f0)
                // reduce
                // second element of Tuple2
                .sum(1);

        result.print().setParallelism(1);

        // execute task
        env.execute();
    }
}
