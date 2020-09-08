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

```java
List<HttpHost> httpHosts = new ArrayList<>();
httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"));
ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<>(
  httpHosts,
  new ElasticsearchSinkFunction<SensorReading> {

    @Override
    public void process(SensorReading t,
                        RuntimeContext runtimeContext,
                        RequestIndexer requestIndexer) {
      System.out.println("saving data: " + t);
      Map<String, String> json = new util.HashMap<>();
      json.put("data", t.toString());
      IndexRequest indexRequest = Requests
        .indexRequest()
        .index("sensor")
        .source(json);
      requestIndexer.add(indexRequest);
      System.out.println("saved successfully");
    }
  }
)

esSinkBuilder.setBulkFlushMaxActions(1);

dataStream.addSink(esSinkBuilder.build());
```

