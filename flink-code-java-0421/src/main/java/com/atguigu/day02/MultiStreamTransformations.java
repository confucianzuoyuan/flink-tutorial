package com.atguigu.day02;

import com.atguigu.day02.util.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

public class MultiStreamTransformations {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<SensorReading> tempReadings = env.addSource(new SensorSource());

        // 并行度设置为1
        DataStream<SmokeLevel> smokeReadings = env.addSource(new SmokeLevelSource()).setParallelism(1);

        tempReadings.keyBy(r -> r.id)
                .connect(smokeReadings.broadcast())
                .flatMap(new RaiseAlertFlatMap())
                .print();

        env.execute();
    }

    public static class RaiseAlertFlatMap implements CoFlatMapFunction<SensorReading, SmokeLevel, Alert> {
        private SmokeLevel smokeLevel = SmokeLevel.LOW;

        @Override
        public void flatMap1(SensorReading sensorReading, Collector<Alert> collector) throws Exception {
            if (this.smokeLevel == SmokeLevel.HIGH && sensorReading.temperature > 0.0) {
                collector.collect(new Alert("报警！" + sensorReading, sensorReading.timestamp));
            }
        }

        @Override
        public void flatMap2(SmokeLevel smokeLevel, Collector<Alert> collector) throws Exception {
            this.smokeLevel = smokeLevel;
        }
    }
}
