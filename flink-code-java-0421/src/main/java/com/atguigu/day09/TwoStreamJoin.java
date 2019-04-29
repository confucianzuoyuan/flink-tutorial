package com.atguigu.day09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

// 两条流的full outer join
public class TwoStreamJoin {

    private static OutputTag<String> unmatchedOrders = new OutputTag<String>("order"){};
    private static OutputTag<String> unmatchedPays   = new OutputTag<String>("pay"){};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        SingleOutputStreamOperator<OrderEvent> orderStream = env
//                .fromElements(
//                        new OrderEvent("order_1", "pay", 1000L),
//                        new OrderEvent("order_2", "pay", 2000L)
//                )
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, OrderEvent>() {
                    @Override
                    public OrderEvent map(String value) throws Exception {
                        String[] arr = value.split(" ");
                        return new OrderEvent(arr[0], arr[1], Long.parseLong(arr[2]));
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderEvent>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                                    @Override
                                    public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                );

        SingleOutputStreamOperator<PayEvent> payStream = env
//                .fromElements(
//                        new PayEvent("order_1", "weixin", 3000L),
//                        new PayEvent("order_3", "weixin", 4000L),
//                        new PayEvent("order_4", "weixin", 10 * 1000L),
//                        new PayEvent("order_2", "weixin", 100 * 1000L)
//                )
                .socketTextStream("localhost", 9998)
                .map(new MapFunction<String, PayEvent>() {
                    @Override
                    public PayEvent map(String value) throws Exception {
                        String[] arr = value.split(" ");
                        return new PayEvent(arr[0], arr[1], Long.parseLong(arr[2]));
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<PayEvent>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<PayEvent>() {
                                    @Override
                                    public long extractTimestamp(PayEvent element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                );

        SingleOutputStreamOperator<String> process = orderStream
                .connect(payStream)
                .keyBy("orderId", "orderId")
                .process(new MatchFunction());

        process.print();

        process.getSideOutput(unmatchedOrders).print();
        process.getSideOutput(unmatchedPays).print();

        env.execute();
    }

    public static class MatchFunction extends CoProcessFunction<OrderEvent, PayEvent, String> {
        private ValueState<OrderEvent> orderState;
        private ValueState<PayEvent> payState;
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            orderState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("order", OrderEvent.class));
            payState = getRuntimeContext().getState(new ValueStateDescriptor<PayEvent>("pay", PayEvent.class));
        }

        @Override
        public void processElement1(OrderEvent orderEvent, Context context, Collector<String> collector) throws Exception {
            if (payState.value() != null) {
                collector.collect("订单ID为 " + orderEvent.orderId + " 的订单实时对账成功！");
                payState.clear();
            } else {
                orderState.update(orderEvent);
                context.timerService().registerEventTimeTimer(orderEvent.timestamp + 5000L);
            }
        }

        @Override
        public void processElement2(PayEvent payEvent, Context context, Collector<String> collector) throws Exception {
            if (orderState.value() != null) {
                collector.collect("订单ID为 " + payEvent.orderId + " 的订单实时对账成功！");
                orderState.clear();
            } else {
                payState.update(payEvent);
                context.timerService().registerEventTimeTimer(payEvent.timestamp + 5000L);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            if (orderState.value() != null) {
                ctx.output(unmatchedOrders, "订单ID为 " + orderState.value().orderId + " 对应的第三方支付没来！");
                orderState.clear();
            }
            if (payState.value() != null) {
                ctx.output(unmatchedPays, "订单ID为 " + payState.value().orderId + " 对应的本地支付事件没来！");
                payState.clear();
            }
        }
    }
}
