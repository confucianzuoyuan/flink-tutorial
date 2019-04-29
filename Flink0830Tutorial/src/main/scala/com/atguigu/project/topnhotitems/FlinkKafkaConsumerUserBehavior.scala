package com.atguigu.project.topnhotitems

import java.sql.Timestamp
import java.util.Properties

import com.atguigu.project.util.{ItemViewCount, UserBehavior}
import org.apache.flink.api.common.functions.{AggregateFunction, RuntimeContext}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

import scala.collection.mutable.ListBuffer

object FlinkKafkaConsumerUserBehavior {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val props = new Properties()
    props.setProperty("bootstrap.servers", "localhost:9092")
    props.setProperty("group.id", "consumer-group")
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("auto.offset.reset", "latest")

    val stream = env
      .addSource(
        new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(), props)
      )
      .map(r => {
        val arr = r.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong * 1000)
      })
      .filter(_.behavior == "pv")
      .assignAscendingTimestamps(_.timestamp)
      .keyBy("itemId")
      .timeWindow(Time.minutes(60), Time.minutes(5))
      .aggregate(new CountAgg, new WindowResultFunction)
      .keyBy("windowEnd")
      .process(new UserBehaviorProcessFunction(3))

    stream.print()

    val httpHosts = new java.util.ArrayList[HttpHost]
    httpHosts.add(new HttpHost("localhost", 9200, "http"))

    val esSinkBuilder = new ElasticsearchSink.Builder[String](
      httpHosts,
      new ElasticsearchSinkFunction[String] {
        def createIndexRequest(element: String) : IndexRequest = {
          val json = new java.util.HashMap[String, String]
          json.put("slidingwindow", element)

          Requests
            .indexRequest()
            .index("user-behavior")
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

  class UserBehaviorProcessFunction(val topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {
    lazy val items = getRuntimeContext.getListState(
      new ListStateDescriptor[ItemViewCount]("items", Types.of[ItemViewCount])
    )

    override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
      items.add(value)
      ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      val allItems = ListBuffer[ItemViewCount]()
      import scala.collection.JavaConversions._
      for (item <- items.get) {
        allItems += item
      }
      items.clear()

      val sortedItems = allItems.sortBy(-_.count).take(topSize)
      val result = new StringBuilder
      result
        .append("=================================================\n")
        .append("time: ")
        .append(new Timestamp(timestamp - 1))
        .append("\n")
      for (i <- sortedItems.indices) {
        val curr = sortedItems(i)
        result
          .append("No.")
          .append(i + 1)
          .append(": ")
          .append(" Item ID = ")
          .append(curr.itemId)
          .append(" pv = ")
          .append(curr.count)
          .append("\n")
      }
      result
        .append("================================================\n\n")
      Thread.sleep(1000)
      out.collect(result.toString)
    }
  }

  class WindowResultFunction extends ProcessWindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
    override def process(key: Tuple, context: Context, elements: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
      val itemId = key.asInstanceOf[Tuple1[Long]].f0
      out.collect(ItemViewCount(itemId, context.window.getEnd, elements.iterator.next()))
    }
  }

  class CountAgg extends AggregateFunction[UserBehavior, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
  }
}
