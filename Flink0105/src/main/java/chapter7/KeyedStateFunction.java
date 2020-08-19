package chapter7;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import util.SensorReading;
import util.SensorSource;
import util.SensorTimeAssigner;

public class KeyedStateFunction {
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // checkpoint every 10 seconds
        env.getCheckpointConfig().setCheckpointInterval(10_000);

        // use event time for the application
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // configure watermark interval
        env.getConfig().setAutoWatermarkInterval(1000L);

        // ingest sensor stream
        DataStream<SensorReading> sensorData = env
                // SensorSource generates random temperature readings
                .addSource(new SensorSource())
                // assign timestamps and watermarks which are required for event time
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());

        KeyedStream<SensorReading, String> keyedSensorData = sensorData.keyBy(r -> r.id);

        DataStream<Tuple3<String, Double, Double>> alerts = keyedSensorData
                .flatMap(new TemperatureAlertFunction(1.7));

        alerts.print();

        env.execute();
    }

    public static class TemperatureAlertFunction extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {

        private ValueState<Double> lastTempState;
        private final Double threshold;

        public TemperatureAlertFunction(Double threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("lastTemp", Types.DOUBLE));
        }

        @Override
        public void flatMap(SensorReading value, Collector<Tuple3<String, Double, Double>> out) throws Exception {
            Double lastTemp = lastTempState.value() != null ? lastTempState.value() : 0.0;

            Double tempDiff = Math.abs(value.temperature - lastTemp);

            if (tempDiff > threshold) {
                out.collect(Tuple3.of(value.id, value.temperature, tempDiff));
            }

            lastTempState.update(value.temperature);
        }
    }
}