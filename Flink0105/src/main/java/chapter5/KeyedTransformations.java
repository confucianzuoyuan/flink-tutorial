package chapter5;

import util.SensorReading;
import util.SensorSource;
import util.SensorTimeAssigner;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyedTransformations {

    public static void main(String[] args) throws Exception {

        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // use event time for the application
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // configure watermark interval
        env.getConfig().setAutoWatermarkInterval(1000L);

        // ingest sensor stream
        DataStream<SensorReading> readings = env
                // SensorSource generates random temperature readings
                .addSource(new SensorSource())
                // assign timestamps and watermarks which are required for event time
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());

        // group sensor readings by sensor id
        KeyedStream<SensorReading, String> keyed = readings
                .keyBy(r -> r.id);

        // a rolling reduce that computes the highest temperature of each sensor and
        // the corresponding timestamp
        DataStream<SensorReading> maxTempPerSensor = keyed
                .reduce((r1, r2) -> {
                    if (r1.temperature > r2.temperature) {
                        return r1;
                    } else {
                        return r2;
                    }
                });

        maxTempPerSensor.print();

        // execute application
        env.execute("Keyed Transformations Example");
    }
}
