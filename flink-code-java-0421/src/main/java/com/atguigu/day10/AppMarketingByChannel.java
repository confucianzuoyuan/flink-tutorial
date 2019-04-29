package com.atguigu.day10;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class AppMarketingByChannel {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        env
                .addSource(new SimulatedEventSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<MarketingUserBehavior>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<MarketingUserBehavior>() {
                            @Override
                            public long extractTimestamp(MarketingUserBehavior element, long recordTimestamp) {
                                return element.ts;
                            }
                        })
                )
                .filter(r -> !r.behavior.equals("UNINSTALL"))
                .map(new MapFunction<MarketingUserBehavior, Tuple2<Tuple2<String, String>, Long>>() {
                    @Override
                    public Tuple2<Tuple2<String, String>, Long> map(MarketingUserBehavior value) throws Exception {
                        return Tuple2.of(Tuple2.of(value.channel, value.behavior), 1L);
                    }
                })
                .keyBy(new KeySelector<Tuple2<Tuple2<String, String>, Long>, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(Tuple2<Tuple2<String, String>, Long> value) throws Exception {
                        return value.f0;
                    }
                })
                .timeWindow(Time.seconds(5), Time.seconds(1))
                .aggregate(new CountAgg(), new MarketingCountByChannel())
                .print();

        env.execute();
    }

    private static class MarketingCountByChannel extends ProcessWindowFunction<Long, String, Tuple2<String, String>, TimeWindow> {
        @Override
        public void process(Tuple2<String, String> stringStringTuple2, Context context, Iterable<Long> iterable, Collector<String> collector) throws Exception {
            collector.collect("窗口结束时间为 " + new Timestamp(context.window().getEnd()) + " 的窗口共有 " + iterable.iterator().next() + " 条数据");
        }
    }

    private static class CountAgg implements AggregateFunction<Tuple2<Tuple2<String, String>, Long>, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<Tuple2<String, String>, Long> value, Long accumulator) {
            return accumulator + 1L;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }
}
