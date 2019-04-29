package com.atguigu.day4;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class KeyedProcessExampleProcessingTime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<String> stream = env.socketTextStream("localhost", 9999);

        stream
                .map(
                        new MapFunction<String, Tuple2<String, String>>() {
                            @Override
                            public Tuple2<String, String> map(String value) throws Exception {
                                String[] arr = value.split(" ");
                                return Tuple2.of(arr[0], arr[1]);
                            }
                        }
                )
                .keyBy(r -> r.f0)
                .process(new MyKeyedPro())
                .print();

        env.execute();
    }

    public static class MyKeyedPro extends KeyedProcessFunction<String, Tuple2<String, String>, String> {
        @Override
        public void processElement(Tuple2<String, String> value, Context ctx, Collector<String> out) throws Exception {
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 10 * 1000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            out.collect("定时器触发了！触发时间是：" + timestamp);
        }
    }
}
