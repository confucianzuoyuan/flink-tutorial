package com.atguigu.day09;

import com.atguigu.day06.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Set;

public class UvStatistics {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<UserBehavior> stream = env
                .readTextFile("/home/zuoyuan/flink-tutorial/flink-code-java-0421/src/main/resources/UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] arr = value.split(",");
                        return new UserBehavior(
                                arr[0],
                                arr[1],
                                arr[2],
                                arr[3],
                                Long.parseLong(arr[4]) * 1000L
                        );
                    }
                })
                .filter(r -> r.behavior.equals("pv"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserBehavior>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                                    @Override
                                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                );

        stream
                .map(new MapFunction<UserBehavior, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map(UserBehavior value) throws Exception {
                        return Tuple2.of("dummy", value.userId);
                    }
                })
                .keyBy(r -> r.f0)
                .timeWindow(Time.hours(1))
                .aggregate(new CountAgg(), new WindowResult())
                .print();

        env.execute();
    }

    public static class WindowResult extends ProcessWindowFunction<Long, String, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<Long> iterable, Collector<String> collector) throws Exception {
            collector.collect("窗口结束时间是：" + new Timestamp(context.window().getEnd()) + " 的窗口的UV是：" + iterable.iterator().next());
        }
    }

    public static class CountAgg implements AggregateFunction<Tuple2<String, String>, Tuple2<Set<String>, Long>, Long> {
        @Override
        public Tuple2<Set<String>, Long> createAccumulator() {
            return Tuple2.of(new HashSet<>(), 0L);
        }

        @Override
        public Tuple2<Set<String>, Long> add(Tuple2<String, String> value, Tuple2<Set<String>, Long> accumulator) {
            if (!accumulator.f0.contains(value.f1)) {
                accumulator.f0.add(value.f1);
                accumulator.f1 += 1;
            }
            return accumulator;
        }

        @Override
        public Long getResult(Tuple2<Set<String>, Long> accumulator) {
            return accumulator.f1;
        }

        @Override
        public Tuple2<Set<String>, Long> merge(Tuple2<Set<String>, Long> a, Tuple2<Set<String>, Long> b) {
            return null;
        }
    }
}
