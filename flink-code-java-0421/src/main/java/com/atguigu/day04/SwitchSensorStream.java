package com.atguigu.day04;

import com.atguigu.day02.util.SensorReading;
import com.atguigu.day02.util.SensorSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class SwitchSensorStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        KeyedStream<SensorReading, String> stream = env
                .addSource(new SensorSource())
                .keyBy(r -> r.id);

        KeyedStream<Tuple2<String, Long>, String> switchStream = env
                .fromElements(Tuple2.of("sensor_2", 10 * 1000L))
                .keyBy(r -> r.f0);

        stream
                .connect(switchStream)
                .process(new SwitchFilter())
                .print();

        env.execute();
    }

    public static class SwitchFilter extends CoProcessFunction<SensorReading, Tuple2<String, Long>, SensorReading> {

        private ValueState<Boolean> forwardingEnabled;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            forwardingEnabled = getRuntimeContext().getState(
                    new ValueStateDescriptor<Boolean>("switch", Types.BOOLEAN)
            );
        }

        @Override
        public void processElement1(SensorReading sensorReading, Context context, Collector<SensorReading> collector) throws Exception {
            // 检查开关是否开启
            if (forwardingEnabled.value() != null && forwardingEnabled.value()) {
                collector.collect(sensorReading);
            }
        }

        @Override
        public void processElement2(Tuple2<String, Long> stringLongTuple2, Context context, Collector<SensorReading> collector) throws Exception {
            forwardingEnabled.update(true);
            context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + stringLongTuple2.f1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<SensorReading> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            forwardingEnabled.clear();
        }
    }
}
