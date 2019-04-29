package com.atguigu.day6;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

public class GenPeriodicWatermarks {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(10 * 1000L);

        DataStream<String> stream = env.socketTextStream("localhost", 9999);

        stream
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        String[] arr = value.split(" ");
                        return Tuple2.of(arr[0], Long.parseLong(arr[1]) * 1000L);
                    }
                })
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {
                    private final Long bound = 10 * 1000L;
                    private Long maxTs = Long.MIN_VALUE + bound + 1;

                    @Override
                    public Watermark getCurrentWatermark() {
                        System.out.println("generate watermark: " + (maxTs - bound - 1) + "ms");
                        return new Watermark(maxTs - bound - 1);
                    }

                    @Override
                    public long extractTimestamp(Tuple2<String, Long> stringLongTuple2, long l) {
                        System.out.println("extract timestamp!!!");
                        maxTs = Math.max(maxTs, stringLongTuple2.f1);
                        return stringLongTuple2.f1;
                    }
                })
                .keyBy(r -> r.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, String>() {
                    @Override
                    public void processElement(Tuple2<String, Long> stringLongTuple2, Context context, Collector<String> collector) throws Exception {
                        collector.collect("current watermark is: " + context.timerService().currentWatermark());
                    }
                })
                .print();

        env.execute();
    }
}
