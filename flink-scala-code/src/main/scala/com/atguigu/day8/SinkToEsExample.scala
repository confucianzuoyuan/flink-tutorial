package com.atguigu.day8

import java.util

import com.atguigu.day2.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

object SinkToEsExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    val httpHosts = new util.ArrayList[HttpHost]()

    httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"))

    val esSinkBuilder = new ElasticsearchSink.Builder[SensorReading](
      httpHosts,
//      new ElasticsearchSinkFunction[SensorReading] {
//        override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
//          val json = new util.HashMap[String, String]()
//          json.put("data", t.temperature.toString)
//
//          val indexRequest = Requests
//            .indexRequest()
//            .index("sensor") // 索引是sensor，相当于数据库
//                        .`type`("readingData") // es6必须写这一行代码
//            .source(json)
//
//          requestIndexer.add(indexRequest)
//        }
//
//      }
      new ElasticsearchSinkFunction[SensorReading] {
        override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          val hashMap = new util.HashMap[String, String]()
          hashMap.put("data", t.toString)

          val indexRequest = Requests
            .indexRequest()
            .index("sensor") // 索引是sensor，相当于数据库
            //            .`type`("readingData") // es6必须写这一行代码
            .source(hashMap)

          requestIndexer.add(indexRequest)
        }
      }

    )
    // 设置每一批写入es多少数据
    esSinkBuilder.setBulkFlushMaxActions(1)

    val stream = env.addSource(new SensorSource)

    stream.addSink(esSinkBuilder.build())

    env.execute()
  }
}