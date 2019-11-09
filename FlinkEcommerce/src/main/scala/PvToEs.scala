import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.util.Collector
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

object PvToEs {

  def main(args: Array[String]): Unit = {
    val resourcesPath = getClass.getResource("/UserBehavior.csv")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val stream = env
      .readTextFile(resourcesPath.getPath)
      .map(line => {
        val linearray = line.split(",")
        UserBehavior(linearray(0).toLong, linearray(1).toLong, linearray(2).toInt, linearray(3), linearray(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000)
      .filter(_.behavior.equals("pv"))
      .map(_ => 1)
      .timeWindowAll(Time.seconds(60 * 60))
      .process(new SumProcess)
//      .print()

    val httpHosts = new java.util.ArrayList[HttpHost]
    httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"))
    //
    val esSinkBuilder = new ElasticsearchSink.Builder[(Long, String)](
      httpHosts,
      new ElasticsearchSinkFunction[(Long, String)] {
        def createIndexRequest(record: (Long, String)): IndexRequest = {
          val json = new java.util.HashMap[String, String]
          json.put("time", record._2.toString)         // timestamp, windowend time
          json.put("cnt", record._1.toString)

          Requests.indexRequest()
            .index("pv-idx")
            .`type`("pv")
            .source(json)
        }

        override def process(record: (Long, String), ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
          indexer.add(createIndexRequest(record))
        }
      }
    )

    stream.addSink(esSinkBuilder.build())


    env.execute("Hot Items Job")
  }

  class SumProcess extends ProcessAllWindowFunction[Int, (Long,String), TimeWindow] {
    override def process(context:  Context, elements: Iterable[Int], out: Collector[(Long, String)]): Unit = {
      var count = 0
      for (e <- elements) {
        count += e
      }
      out.collect((count,context.window.getEnd.toString))
    }
  }

}