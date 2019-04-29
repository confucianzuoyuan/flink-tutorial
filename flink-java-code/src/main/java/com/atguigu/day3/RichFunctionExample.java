package com.atguigu.day3;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RichFunctionExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Integer> stream = env.fromElements(1,2,3);

        stream
                .map(new RichMapFunction<Integer, Integer>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        System.out.println("enter lifecycle");
                    }

                    @Override
                    public Integer map(Integer value) throws Exception {
                        return value + 1;
                    }

                    @Override
                    public void close() throws Exception {
                        System.out.println("exit lifecycle");
                    }
                })
                .print();

        env.execute();
    }
}
