### ElasticSearch

在主函数中调用：

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-elasticsearch6_2.11</artifactId>
  <version>${flink.version}</version>
</dependency>
```

可选依赖：

```xml
<dependency>
	<groupId>org.elasticsearch.client</groupId>
	<artifactId>elasticsearch-rest-high-level-client</artifactId>
	<version>7.9.1</version>

</dependency>
<dependency>
	<groupId>org.elasticsearch</groupId>
	<artifactId>elasticsearch</artifactId>
	<version>7.9.1</version>
</dependency>
```

示例代码：

**scala version**

```scala
object SinkToES {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"))
    val esSinkBuilder = new ElasticsearchSink.Builder[SensorReading](
      httpHosts,
      new ElasticsearchSinkFunction[SensorReading] {
        override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          val hashMap = new util.HashMap[String, String]()
          hashMap.put("data", t.toString)

          val indexRequest = Requests
            .indexRequest()
            .index("sensor") // 索引是sensor，相当于数据库
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
```

**java version**

```java
public class SinkToES {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"));

        ElasticsearchSink.Builder<SensorReading> sensorReadingBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<SensorReading>() {
                    @Override
                    public void process(SensorReading sensorReading, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                        HashMap<String, String> map = new HashMap<>();
                        map.put("data", sensorReading.toString());

                        IndexRequest indexRequest = Requests
                                .indexRequest()
                                .index("sensor") // 索引是sensor，相当于数据库
                                .source(map);

                        requestIndexer.add(indexRequest);
                    }
                }
        );

        sensorReadingBuilder.setBulkFlushMaxActions(1);

        DataStream<SensorReading> stream = env.addSource(new SensorSource());

        stream.addSink(sensorReadingBuilder.build());

        env.execute();
    }
}
```

