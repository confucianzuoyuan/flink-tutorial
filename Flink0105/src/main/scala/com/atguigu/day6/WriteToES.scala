package com.atguigu.day6

import java.util

import com.atguigu.day2.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

object WriteToES {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 有关es的配置的样板代码
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("localhost", 9200))
    val esSinkBuilder = new ElasticsearchSink.Builder[SensorReading](
      httpHosts,
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