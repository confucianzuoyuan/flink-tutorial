package com.atguigu.refactorcode;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class TwoStream {

    private static OutputTag<String> unmatchedLefts = new OutputTag<String>("left"){};
    private static OutputTag<String> unmatchedRights   = new OutputTag<String>("right"){};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, String>> leftStream = env
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map(String value) throws Exception {
                        String[] arr = value.split(" ");
                        return Tuple2.of(arr[0], arr[1]);
                    }
                });

        SingleOutputStreamOperator<Tuple2<String, String>> rightStream = env
                .socketTextStream("localhost", 9998)
                .map(new MapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map(String value) throws Exception {
                        String[] arr = value.split(" ");
                        return Tuple2.of(arr[0], arr[1]);
                    }
                });

        SingleOutputStreamOperator<String> process = leftStream.keyBy(r -> r.f0)
                .connect(rightStream.keyBy(r -> r.f0))
                .process(new MatchFunction());

        process.print();

        process.getSideOutput(unmatchedLefts).print();
        process.getSideOutput(unmatchedRights).print();

        env.execute();
    }

    public static class MatchFunction extends CoProcessFunction<Tuple2<String, String>, Tuple2<String, String>, String> {
        private ValueState<Tuple2<String, String>> leftState;
        private ValueState<Tuple2<String, String>> rightState;
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            leftState = getRuntimeContext().getState(new ValueStateDescriptor<Tuple2<String, String>>("left", Types.TUPLE(Types.STRING, Types.STRING)));
            rightState = getRuntimeContext().getState(new ValueStateDescriptor<Tuple2<String, String>>("right", Types.TUPLE(Types.STRING, Types.STRING)));
        }

        @Override
        public void processElement1(Tuple2<String, String> left, Context context, Collector<String> collector) throws Exception {
            if (rightState.value() != null) {
                collector.collect("左边和右边匹配成功！ ");
                rightState.clear();
            } else {
                leftState.update(left);
                context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + 5000L);
            }
        }

        @Override
        public void processElement2(Tuple2<String, String> right, Context context, Collector<String> collector) throws Exception {
            if (leftState.value() != null) {
                collector.collect("右边和左边匹配成功！");
                leftState.clear();
            } else {
                rightState.update(right);
                context.timerService().registerEventTimeTimer(context.timerService().currentProcessingTime() + 5000L);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            if (leftState.value() != null) {
                ctx.output(unmatchedLefts, "只有左边没有右边！");
                leftState.clear();
            }
            if (rightState.value() != null) {
                ctx.output(unmatchedRights, "只有右边没有左边！");
                rightState.clear();
            }
        }
    }
}
