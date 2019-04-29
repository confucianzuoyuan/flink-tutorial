package com.atguigu.day05;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class UpdateWindowResultWithLateEvent {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> stream = env.socketTextStream("localhost", 9999);

        stream
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String s) throws Exception {
                        String[] arr = s.split(" ");
                        return Tuple2.of(arr[0], Long.parseLong(arr[1]) * 1000L);
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, Long> stringLongTuple2, long l) {
                                return stringLongTuple2.f1;
                            }
                        })
                )
                .keyBy(r -> r.f0)
                .timeWindow(Time.seconds(5))
                .allowedLateness(Time.seconds(5))
                .process(new UpdateWindowResult())
                .print();

        env.execute();
    }

    public static class UpdateWindowResult extends ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<Tuple2<String, Long>> iterable, Collector<String> collector) throws Exception {
            long count = 0L;
            for (Tuple2<String, Long> i : iterable) {
                count += 1;
            }

            // 可见范围比getRuntimeContext.getState更小，只对当前key、当前window可见
            // 基于窗口的状态变量，只能当前key和当前窗口访问
            ValueState<Boolean> isUpdate = context.windowState().getState(
                    new ValueStateDescriptor<Boolean>("isUpdate", Types.BOOLEAN)
            );

            // 当水位线超过窗口结束时间时，触发窗口的第一次计算！
            if (isUpdate.value() == null) {
                collector.collect("窗口第一次触发计算！一共有 " + count + " 条数据！");
                isUpdate.update(true);
            } else {
                collector.collect("窗口更新了！一共有 " + count + " 条数据！");
            }
        }
    }
}
