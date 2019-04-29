package com.atguigu.day02;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RichFunctionExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> stream = env.fromElements(1,2,3,4);

        stream
                .map(new RichMapFunction<Integer, Integer>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        System.out.println("进入生命周期");
                    }

                    @Override
                    public Integer map(Integer integer) throws Exception {
                        return integer + 10;
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        System.out.println("离开生命周期");
                    }
                })
                .print();


        env.execute();
    }
}
