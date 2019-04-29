package com.atguigu.day3;

import com.atguigu.day2.SensorReading;
import com.atguigu.day2.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

public class MultiStreamTransformations {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<SensorReading> tempReadings = env.addSource(new SensorSource());

        DataStream<SmokeLevel> smokeReadings = env
                .addSource(new SmokeLevelSource())
                .setParallelism(1);

        KeyedStream<SensorReading, String> keyedTempReadings = tempReadings.keyBy(r -> r.id);

        DataStream<Alert> alerts = keyedTempReadings
                .connect(smokeReadings.broadcast())
                .flatMap(new RaiseAlertFlatMap());

        alerts.print();

        env.execute();
    }

    public static class RaiseAlertFlatMap implements CoFlatMapFunction<SensorReading, SmokeLevel, Alert> {
        private SmokeLevel smokeLevel = SmokeLevel.LOW;

        @Override
        public void flatMap1(SensorReading value, Collector<Alert> out) throws Exception {
            if (this.smokeLevel == SmokeLevel.HIGH && value.temperature > -100.0) {
                out.collect(new Alert("报警了！" + value, value.timestamp));
            }
        }

        @Override
        public void flatMap2(SmokeLevel value, Collector<Alert> out) throws Exception {
            this.smokeLevel = value;
        }
    }
}
