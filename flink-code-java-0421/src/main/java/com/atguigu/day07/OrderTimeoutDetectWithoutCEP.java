package com.atguigu.day07;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class OrderTimeoutDetectWithoutCEP {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        SingleOutputStreamOperator<OrderEvent> stream = env
                .fromElements(
                        new OrderEvent("order_1", "create", 1000L),
                        new OrderEvent("order_2", "create", 2000L),
                        new OrderEvent("order_1", "pay", 3000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderEvent>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                                    @Override
                                    public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                                        return element.eventTime;
                                    }
                                })
                );

        stream
                .keyBy(r -> r.orderId)
                .process(new MyKeyed())
                .print();

        env.execute();
    }

    public static class MyKeyed extends KeyedProcessFunction<String, OrderEvent, String> {

        private ValueState<OrderEvent> orderState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            orderState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("order", OrderEvent.class));
        }

        @Override
        public void processElement(OrderEvent orderEvent, Context context, Collector<String> collector) throws Exception {
            if (orderEvent.eventType.equals("create")) {
                // 为什么判空？因为pay事件可能先到
                if (orderState.value() == null) {
                    orderState.update(orderEvent);
                    context.timerService().registerEventTimeTimer(orderEvent.eventTime + 5000L);
                }
            } else {
                orderState.update(orderEvent);
                collector.collect("订单ID为 " + orderEvent.orderId + " 支付成功！");
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            if (orderState.value() != null && orderState.value().eventType.equals("create")) {
                out.collect("没有支付的订单ID是 " + orderState.value().orderId);
            }
            orderState.clear();
        }
    }
}
