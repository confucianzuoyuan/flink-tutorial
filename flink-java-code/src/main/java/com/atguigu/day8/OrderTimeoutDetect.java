package com.atguigu.day8;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

public class OrderTimeoutDetect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStream<OrderEvent> stream = env
                .fromElements(
                        new OrderEvent("order_1", "create", 2000L),
                        new OrderEvent("order_2", "create", 3000L),
                        new OrderEvent("order_1", "pay", 4000L),
                        new OrderEvent("order_2", "pay", 10000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderEvent>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                            @Override
                            public long extractTimestamp(OrderEvent orderEvent, long l) {
                                return orderEvent.OrderTime;
                            }
                        })
                );

        Pattern<OrderEvent, OrderEvent> pattern = Pattern
                .<OrderEvent>begin("create")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent orderEvent) throws Exception {
                        return orderEvent.OrderType.equals("create");
                    }
                })
                .next("pay")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent orderEvent) throws Exception {
                        return orderEvent.OrderType.equals("pay");
                    }
                })
                .within(Time.seconds(5));

        PatternStream<OrderEvent> patternedStream = CEP.pattern(stream.keyBy(r -> r.OrderId), pattern);

        SingleOutputStreamOperator<String> resultStream = patternedStream
                .flatSelect(
                        new OutputTag<String>("timeout") {
                        },
                        new PatternFlatTimeoutFunction<OrderEvent, String>() {
                            @Override
                            public void timeout(Map<String, List<OrderEvent>> map, long l, Collector<String> collector) throws Exception {
                                collector.collect(map.get("create").iterator().next().OrderId + " is not payed!");
                            }
                        },
                        new PatternFlatSelectFunction<OrderEvent, String>() {
                            @Override
                            public void flatSelect(Map<String, List<OrderEvent>> map, Collector<String> collector) throws Exception {
                                collector.collect(map.get("pay").iterator().next().OrderId + " is payed!");
                            }
                        });

        resultStream.print();
        resultStream.getSideOutput(new OutputTag<String>("timeout") {}).print();

        env.execute();
    }
}
