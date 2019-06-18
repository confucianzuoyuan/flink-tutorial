import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.util.Collector
import scala.collection.Map

object OrderTimeout {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val orderEventStream = env.fromCollection(List(
      OrderEvent("1", "create", "1558430842"),
      OrderEvent("2", "create", "1558430843"),
      OrderEvent("2", "pay", "1558430844"),
      OrderEvent("3", "pay", "1558430942"),
      OrderEvent("4", "pay", "1558430943")
    )).assignAscendingTimestamps(_.eventTime.toLong * 1000)

//    val orders: DataStream[String] = env.socketTextStream("localhost", 9999)
//
//    val orderEventStream = orders
//      .map(s => {
//        println(s)
//        val slist = s.split("\\|")
//        println(slist)
//        OrderEvent(slist(0), slist(1), slist(2))
//      })
//      .assignAscendingTimestamps(_.eventTime.toLong * 1000)

    val orderPayPattern = Pattern.begin[OrderEvent]("begin")
      .where(_.eventType.equals("create"))
      .next("next")
      .where(_.eventType.equals("pay"))
      .within(Time.seconds(5))

    val orderTimeoutOutput = OutputTag[OrderEvent]("orderTimeout")

    val patternStream = CEP.pattern(orderEventStream.keyBy("orderId"), orderPayPattern)

    val timeoutFunction = (map: Map[String, Iterable[OrderEvent]], timestamp: Long, out: Collector[OrderEvent]) => {
      print(timestamp)
      val orderStart = map.get("begin").get.head
      out.collect(orderStart)
    }

    val selectFunction = (map: Map[String, Iterable[OrderEvent]], out: Collector[OrderEvent]) => {
    }

    val timeoutOrder = patternStream.flatSelect(orderTimeoutOutput)(timeoutFunction)(selectFunction)

    timeoutOrder.getSideOutput(orderTimeoutOutput).print()

    env.execute

  }
}

