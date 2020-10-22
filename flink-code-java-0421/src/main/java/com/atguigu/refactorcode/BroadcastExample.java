package com.atguigu.refactorcode;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class BroadcastExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<Long> broadcastValue = env
                .fromElements(1L)
                .setParallelism(1);

        broadcastValue
                .broadcast()
                .map(new MapFunction<Long, Long>() {
                    @Override
                    public Long map(Long value) throws Exception {
                        long id = Thread.currentThread().getId();
                        System.out.println("线程id：" + id + ", 接收到数据：" + value);
                        return value;
                    }
                });

        env.execute();
    }

}
