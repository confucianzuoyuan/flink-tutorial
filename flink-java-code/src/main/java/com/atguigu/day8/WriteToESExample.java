package com.atguigu.day8;

import com.atguigu.day2.SensorReading;
import com.atguigu.day2.SensorSource;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

public class WriteToESExample {
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
