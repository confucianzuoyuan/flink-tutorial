package com.atguigu.day8

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

object FlinkKafkaProducerExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val conProperties = new Properties()
    conProperties.setProperty("bootstrap.servers", "localhost:9092")
    conProperties.setProperty("group.id", "consumer-group")
    conProperties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    conProperties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    conProperties.setProperty("auto.offset.reset", "latest")

    val stream = env.addSource(new FlinkKafkaConsumer011[String](
      "test",
      new SimpleStringSchema(),
      conProperties
    ))

    val prodProperties = new Properties()
    prodProperties.setProperty("bootstrap.servers", "localhost:9092")

    stream
      .addSink(new FlinkKafkaProducer011[String](
        "test",
        new SimpleStringSchema(),
        prodProperties
      ))

    stream.print()

    env.execute()
  }
}