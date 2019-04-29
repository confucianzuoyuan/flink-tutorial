package com.atguigu.day04;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class PeriodicWatermarkGenerator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> stream = env.socketTextStream("localhost", 9999);

        stream
                .map(r -> Tuple2.of(r.split(" ")[0], Long.parseLong(r.split(" ")[1]) * 1000L))
                .returns(new TypeHint<Tuple2<String, Long>>() {})
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {
                    final long bound = 5 * 1000L;
                    long maxTs = Long.MIN_VALUE + bound + 1;

                    // 系统在流中插入水位线时执行，默认200ms执行一次
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(maxTs - bound - 1);
                    }

                    // 每来一条数据执行一次
                    @Override
                    public long extractTimestamp(Tuple2<String, Long> stringLongTuple2, long l) {
                        maxTs = Math.max(maxTs, stringLongTuple2.f1); // 更新观察到的最大的事件时间
                        return stringLongTuple2.f1; // 告诉系统哪一个字段是事件时间
                    }
                })
                .keyBy(r -> r.f0)
                .process(new Keyed())
                .print();

        env.execute();
    }

    public static class Keyed extends KeyedProcessFunction<String, Tuple2<String, Long>, String> {
        @Override
        public void processElement(Tuple2<String, Long> stringLongTuple2, Context context, Collector<String> collector) throws Exception {
            context.timerService().registerEventTimeTimer(stringLongTuple2.f1 + 10 * 1000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            out.collect("时间戳是：" + new Timestamp(timestamp) + " 的定时器触发了！");
        }
    }
}
