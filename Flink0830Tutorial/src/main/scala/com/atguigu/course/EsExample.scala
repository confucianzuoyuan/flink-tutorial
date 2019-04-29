package com.atguigu.course

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

object EsExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env
      .socketTextStream("localhost", 9999, '\n')

    val httpHosts = new util.ArrayList[HttpHost]
    httpHosts.add(new HttpHost("localhost", 9200, "http"))

    val esSinkBuilder = new ElasticsearchSink.Builder[String](
      httpHosts,
      new ElasticsearchSinkFunction[String] {
        def createIndexRequest(element: String) : IndexRequest = {
          val json = new util.HashMap[String, String]
          json.put("atguigu", element)

          Requests
            .indexRequest()
            .index("my-index1")
            .`type`("my-type")
            .source(json)
        }

        override def process(t: String, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          requestIndexer.add(createIndexRequest(t))
        }
      }
    )

    // 批量写入es的操作，设置为 1 的意思：来一条数据，往 es 里面写入一条数据
    esSinkBuilder.setBulkFlushMaxActions(1)

    stream.addSink(esSinkBuilder.build())

    env.execute()
  }
}
