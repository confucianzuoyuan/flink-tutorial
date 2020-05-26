### ElasticSearch

在主函数中调用：

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-elasticsearch6_2.11</artifactId>
  <version>${flink.version}</version>
</dependency>
```

在主函数中调用：

```scala
val httpHosts = new util.ArrayList[HttpHost]()
httpHosts.add(new HttpHost("localhost", 9200))
val esSinkBuilder = new ElasticsearchSink.Builder[SensorReading](
  httpHosts,
  new ElasticsearchSinkFunction[SensorReading] {
    override def process(t: SensorReading,
                         runtimeContext: RuntimeContext,
                         requestIndexer: RequestIndexer): Unit = {
      println("saving data: " + t)
      val json = new util.HashMap[String, String]()
      json.put("data", t.toString)
      val indexRequest = Requests
        .indexRequest()
        .index("sensor")
        .`type`("readingData")
        .source(json)
      requestIndexer.add(indexRequest)
      println("saved successfully")
    }
  })
dataStream.addSink(esSinkBuilder.build())
```

