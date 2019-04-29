package com.atguigu.day03;

import com.atguigu.day02.util.SensorReading;
import com.atguigu.day02.util.SensorSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class TempIncreaseAlert {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());

        stream
                .keyBy(r -> r.id)
                .process(new TempIncrease())
                .print();

        env.execute();
    }

    public static class TempIncrease extends KeyedProcessFunction<String, SensorReading, String> {
        private ValueState<Double> lastTemp; // 最近一次温度值
        private ValueState<Long> currentTimer; // 定时器的时间戳

        // 状态变量可见范围：当前元素所对应的key的支流
        // 单例，状态变量只会被初始化一次
        // 状态变量经由检查点操作保存在状态后端（例如：HDFS）
        // flink程序宕机重启之后，会试图去状态后端获取状态变量，如果获取不到，则初始化状态变量
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            lastTemp = getRuntimeContext().getState(
                    new ValueStateDescriptor<Double>("last-temp", Types.DOUBLE)
            );
            currentTimer = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("timer", Types.LONG)
            );
        }

        // 2, 3, 4, 1
        @Override
        public void processElement(SensorReading r, Context ctx, Collector<String> collector) throws Exception {
            // 初始化一个prevTemp变量，用来保存从状态变量里取出的最近一次的温度值
            Double prevTemp = 0.0;
            // 什么时候lastTemp.value()是null？当第一条温度来的时候
            if (lastTemp.value() != null) {
                prevTemp = lastTemp.value();
            }
            // 更新状态变量
            lastTemp.update(r.temperature);

            Long curTimerTimestamp = 0L;
            if (currentTimer.value() != null) {
                curTimerTimestamp = currentTimer.value();
            }

            if (prevTemp == 0.0 || r.temperature < prevTemp) {
                // 删除curTimerTimestamp对应的定时器
                ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp);
                currentTimer.clear();
            } else if (r.temperature > prevTemp && curTimerTimestamp == 0L) {
                // 当前机器时间 + 1s
                long oneSecondLater = ctx.timerService().currentProcessingTime() + 1000L;
                // 注册处理时间定时器
                ctx.timerService().registerProcessingTimeTimer(oneSecondLater);
                currentTimer.update(oneSecondLater);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            out.collect("传感器id为：" + ctx.getCurrentKey() + "的传感器连续1s温度上升！");
            currentTimer.clear();
        }
    }
}
