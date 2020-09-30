package com.atguigu.day05;

import com.atguigu.day02.util.SensorReading;
import com.atguigu.day02.util.SensorSource;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class ListStateExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());

        stream
                .filter(r -> r.id.equals("sensor_1"))
                .keyBy(r -> r.id)
                .process(new KeyedProcessFunction<String, SensorReading, String>() {
                    private ValueState<Long> timer;
                    private ListState<SensorReading> readingList;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        timer = getRuntimeContext().getState(
                                new ValueStateDescriptor<Long>("timer", Types.LONG)
                        );
                        readingList = getRuntimeContext().getListState(
                                new ListStateDescriptor<SensorReading>("reading-list", SensorReading.class)
                        );
                    }

                    @Override
                    public void processElement(SensorReading sensorReading, Context context, Collector<String> collector) throws Exception {
                        readingList.add(sensorReading);
                        if (timer.value() == null) {
                            context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + 10 * 1000L);
                            timer.update(context.timerService().currentProcessingTime() + 10 * 1000L);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        long count = 0L;
                        for (SensorReading r : readingList.get()) {
                            count += 1;
                        }
                        timer.clear();
                        out.collect("当前流中有 " + count + " 条元素");
                    }
                })
                .print();

        env.execute();
    }
}
