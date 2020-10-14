package com.atguigu.day10;

import com.atguigu.day02.util.SensorReading;
import com.atguigu.day02.util.SensorSource;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkMySQLjoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());

        stream
                .map(new RichMapFunction<SensorReading, String>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        // 读取mysql的表的数据
                        // 如果mysql中表的数据偶尔有更新，怎么办？
                        // 再开一个线程，定时去重新读取mysql表中的数据
                    }

                    @Override
                    public String map(SensorReading value) throws Exception {
                        // 每来一条元素，和open方法中读取的mysql的数据join一次
                        // 流和维表的join
                        return null;
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                    }
                })
                .print();

        env.execute();
    }
}
