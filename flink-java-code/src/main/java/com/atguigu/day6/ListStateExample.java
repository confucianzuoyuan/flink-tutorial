package com.atguigu.day6;

import com.atguigu.day2.SensorReading;
import com.atguigu.day2.SensorSource;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class ListStateExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new SensorSource())
                .filter(r -> r.id.equals("sensor_1"))
                .keyBy(r -> r.id)
                .process(new KeyedProcessFunction<String, SensorReading, String>() {

                    private ListState<SensorReading> readings;
                    private ValueState<Long> timerTs;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        readings = getRuntimeContext().getListState(
                                new ListStateDescriptor<SensorReading>("readings", SensorReading.class)
                        );
                        timerTs = getRuntimeContext().getState(
                                new ValueStateDescriptor<Long>("ts", Types.LONG)
                        );
                    }

                    @Override
                    public void processElement(SensorReading sensorReading, Context context, Collector<String> collector) throws Exception {
                        readings.add(sensorReading);
                        if (timerTs.value() == null) {
                            context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + 10 * 1000L);
                            timerTs.update(context.timerService().currentProcessingTime() + 10 * 1000L);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        long count = 0L;
                        for(SensorReading r : readings.get()) {
                            count++;
                        }
                        out.collect("there are " + count + " readings");
                        timerTs.clear();
                    }
                })
                .print();

        env.execute();
    }
}
