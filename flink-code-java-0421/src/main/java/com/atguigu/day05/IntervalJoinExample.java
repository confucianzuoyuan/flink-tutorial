package com.atguigu.day05;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class IntervalJoinExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        KeyedStream<Tuple3<String, Long, String>, String> clickStream = env
                .fromElements(
                        Tuple3.of("user_1", 10 * 60 * 1000L, "click"),
                        Tuple3.of("user_1", 16 * 60 * 1000L, "click"),
                        Tuple3.of("user_2", 20 * 60 * 1000L, "click")
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, Long, String>>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Long, String>>() {
                                    @Override
                                    public long extractTimestamp(Tuple3<String, Long, String> click, long l) {
                                        return click.f1;
                                    }
                                })
                )
                .keyBy(r -> r.f0);

        KeyedStream<Tuple3<String, Long, String>, String> browseStream = env
                .fromElements(
                        Tuple3.of("user_1", 5 * 60 * 1000L, "browse"),
                        Tuple3.of("user_1", 6 * 60 * 1000L, "browse")
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, Long, String>>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Long, String>>() {
                                    @Override
                                    public long extractTimestamp(Tuple3<String, Long, String> browse, long l) {
                                        return browse.f1;
                                    }
                                })
                )
                .keyBy(r -> r.f0);

        clickStream
                .intervalJoin(browseStream)
                .between(Time.minutes(-10), Time.minutes(0))
                .process(new ProcessJoinFunction<Tuple3<String, Long, String>, Tuple3<String, Long, String>, String>() {
                    @Override
                    public void processElement(Tuple3<String, Long, String> click, Tuple3<String, Long, String> browse, Context context, Collector<String> collector) throws Exception {
                        collector.collect(click + " => " + browse);
                    }
                })
                .print();

        env.execute();
    }
}
