package com.atguigu.day06;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.util.Properties;
import java.util.Scanner;

public class UserBehaviorProduceToKafka {
    public static void main(String[] args) throws Exception {
        writeToKafka("hotitems");
    }

    public static void writeToKafka(String topic) throws Exception {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        // pass the path to the file as a parameter
        File file =
                new File("/home/zuoyuan/flink-tutorial/flink-code-java-0421/src/main/resources/UserBehavior.csv");
        Scanner sc = new Scanner(file);

        while (sc.hasNextLine()) {
            producer.send(new ProducerRecord<String, String>(topic, sc.nextLine()));
        }
    }
}
