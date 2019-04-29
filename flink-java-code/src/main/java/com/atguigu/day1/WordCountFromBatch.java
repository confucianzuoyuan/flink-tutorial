package com.atguigu.day1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class WordCountFromBatch {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStream<String> stream = env.fromElements("Hello World", "Hello World");

        stream
                .flatMap(new Tokenizer())
                .keyBy(r -> r.f0)
                .sum(1)
                .print()
                .setParallelism(1);

        env.execute("Flink Streaming Java API Skeleton");
    }

    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] stringList = value.split("\\s");
            for (String s : stringList) {
                // 使用out.collect方法向下游发送数据
                out.collect(new Tuple2(s, 1));
            }
        }
    }
}
