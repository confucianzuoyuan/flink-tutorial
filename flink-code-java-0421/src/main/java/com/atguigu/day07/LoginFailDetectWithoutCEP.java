package com.atguigu.day07;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class LoginFailDetectWithoutCEP {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<LoginEvent> stream = env
                .fromElements(
                        new LoginEvent("user_1", "0.0.0.0", "fail", 2000L),
                        new LoginEvent("user_1", "0.0.0.1", "fail", 3000L),
                        new LoginEvent("user_1", "0.0.0.4", "success", 3500L),
                        new LoginEvent("user_1", "0.0.0.2", "fail", 4000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<LoginEvent>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                                    @Override
                                    public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                                        return element.eventTime;
                                    }
                                })
                );

        stream
                .keyBy(r -> r.userId)
                .process(new MyKeyed())
                .print();

        env.execute();
    }

    public static class MyKeyed extends KeyedProcessFunction<String, LoginEvent, String> {
        private ListState<LoginEvent> loginListState;
        private ValueState<Long> timerTs;
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            loginListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<LoginEvent>("login-list", LoginEvent.class)
            );
            timerTs = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("timer", Types.LONG)
            );
        }

        @Override
        public void processElement(LoginEvent loginEvent, Context context, Collector<String> collector) throws Exception {
            if (loginEvent.eventType.equals("success")) {
                loginListState.clear();
                if (timerTs.value() != null) {
                    context.timerService().deleteEventTimeTimer(timerTs.value());
                    timerTs.clear();
                }
            } else {
                if (timerTs.value() == null) {
                    loginListState.add(loginEvent);
                    context.timerService().registerEventTimeTimer(loginEvent.eventTime + 5000L);
                    timerTs.update(loginEvent.eventTime + 5000L);
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            if (loginListState.get() != null) {
                long count = 0L;
                for (LoginEvent e : loginListState.get()) {
                    count += 1;
                }
                if (count > 2) {
                    out.collect("用户ID为 " + ctx.getCurrentKey() + " 的用户恶意登录！");
                }
            }
            loginListState.clear();
            timerTs.clear();
        }
    }
}
