package com.atguigu.day4;

import com.atguigu.day2.SensorReading;
import com.atguigu.day2.SensorSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class TempIncreaseAlert {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<SensorReading> stream = env.addSource(new SensorSource());

        stream
                .keyBy(r -> r.id)
                .process(new TempIncrease())
                .print();

        env.execute();
    }

    public static class TempIncrease extends KeyedProcessFunction<String, SensorReading, String> {
        private ValueState<Double> lastTemp;
        private ValueState<Long> timer;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            lastTemp = getRuntimeContext().getState(
                    new ValueStateDescriptor<Double>("last-temp", Types.DOUBLE)
            );
            timer = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("timer", Types.LONG)
            );
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            Double prevTemp = 0.0;
            if (lastTemp.value() != null) {
                prevTemp = lastTemp.value();
            }

            lastTemp.update(value.temperature);

            Long ts = 0L;
            if (timer.value() != null) {
                ts = timer.value();
            }

            if (prevTemp == 0.0 || value.temperature < prevTemp) {
                ctx.timerService().deleteProcessingTimeTimer(ts);
                timer.clear();
            } else if (value.temperature > prevTemp && ts == 0) {
                long oneSencondLater = ctx.timerService().currentProcessingTime() + 1000L;
                ctx.timerService().registerProcessingTimeTimer(oneSencondLater);
                timer.update(oneSencondLater);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            out.collect("传感器ID是 " + ctx.getCurrentKey() + " 的传感器的温度连续1s上升了！");
            timer.clear();
        }
    }
}
