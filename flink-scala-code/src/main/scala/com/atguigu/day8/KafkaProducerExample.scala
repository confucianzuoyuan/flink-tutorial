package com.atguigu.day8

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaProducerExample {
  def main(args: Array[String]): Unit = {
    writeToKafka("atguigu")
  }

  def writeToKafka(topic: String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put(
      "key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer"
    )
    props.put(
      "value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer"
    )
    val producer = new KafkaProducer[String, String](props)
    val data = io.Source.fromFile("/home/zuoyuan/flink-tutorial/flink-scala-code/src/main/resources/UserBehavior.csv")
    for (line <- data.getLines()) {
      val record = new ProducerRecord[String, String](topic, line)
      producer.send(record)
    }
//    producer.send(new ProducerRecord[String, String]("test", "hello world"))
    producer.close()
  }
}