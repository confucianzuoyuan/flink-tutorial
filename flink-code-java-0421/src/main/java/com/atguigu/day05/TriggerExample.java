package com.atguigu.day05;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class TriggerExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        env
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String s) throws Exception {
                        String[] arr = s.split(" ");
                        return Tuple2.of(arr[0], Long.parseLong(arr[1]) * 1000L);
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, Long> stringLongTuple2, long l) {
                                return stringLongTuple2.f1;
                            }
                        })
                )
                .keyBy(r -> r.f0)
                .timeWindow(Time.seconds(5))
                .trigger(new OneSecondIntervalTrigger())
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, Long>> iterable, Collector<String> collector) throws Exception {
                        long count = 0L;
                        for (Tuple2<String, Long> i : iterable) count += 1;
                        collector.collect("窗口中有 " + count + " 条元素");
                    }
                })
                .print();

        env.execute();
    }

    public static class OneSecondIntervalTrigger extends Trigger<Tuple2<String, Long>, TimeWindow> {
        // 来一条调用一次
        @Override
        public TriggerResult onElement(Tuple2<String, Long> r, long l, TimeWindow window, TriggerContext ctx) throws Exception {
            ValueState<Boolean> firstSeen = ctx.getPartitionedState(
                    new ValueStateDescriptor<Boolean>("first-seen", Types.BOOLEAN)
            );

            if (firstSeen.value() == null) {
                // 4999 + (1000 - 4999 % 1000) = 5000
                System.out.println("第一条数据来的时候 ctx.getCurrentWatermark() 的值是 " + ctx.getCurrentWatermark());
                long t = ctx.getCurrentWatermark() + (1000L - ctx.getCurrentWatermark() % 1000L);
                ctx.registerEventTimeTimer(t);
                ctx.registerEventTimeTimer(window.getEnd());
                firstSeen.update(true);
            }
            return TriggerResult.CONTINUE;
        }

        // 定时器逻辑
        @Override
        public TriggerResult onEventTime(long ts, TimeWindow window, TriggerContext ctx) throws Exception {
            if (ts == window.getEnd()) {
                return TriggerResult.FIRE_AND_PURGE;
            } else {
                System.out.println("当前水位线是：" + ctx.getCurrentWatermark());
                long t = ctx.getCurrentWatermark() + (1000L - ctx.getCurrentWatermark() % 1000L);
                if (t < window.getEnd()) {
                    ctx.registerEventTimeTimer(t);
                }
                return TriggerResult.FIRE;
            }
        }

        @Override
        public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow timeWindow, TriggerContext ctx) throws Exception {
            ValueState<Boolean> firstSeen = ctx.getPartitionedState(
                    new ValueStateDescriptor<Boolean>("first-seen", Types.BOOLEAN)
            );
            firstSeen.clear();
        }
    }
}
