package com.atguigu.day8;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;

public class FlinkKafkaProducerExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        Properties prodProperties = new Properties();
        prodProperties.put("bootstrap.servers", "localhost:9092");

        DataStream<String> stream = env.addSource(new FlinkKafkaConsumer011<String>(
                "test",
                new SimpleStringSchema(),
                properties
        ));

        stream.addSink(new FlinkKafkaProducer011<String>(
                "test",
                new SimpleStringSchema(),
                prodProperties
        ));

        stream.print();

        env.execute();
    }
}
