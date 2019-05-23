import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.TimeCharacteristic

import scala.collection.Map

case class OrderEvent(orderId: String, eventType: String, eventTime: String)

case class OrderTimeout(orderId: String, eventType: String)

object OrderTimeout {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val orderEventStream = env.fromCollection(List(
      OrderEvent("1", "create", "1558430842"),
      OrderEvent("2", "create", "1558430843"),
      OrderEvent("2", "pay", "1558430844")
    )).assignAscendingTimestamps(_.eventTime.toLong)

    val orderPayPattern = Pattern.begin[OrderEvent]("begin")
      .where(_.eventType.equals("create"))
      .next("next")
      .where(_.eventType.equals("pay"))
      .within(Time.seconds(5))

    val orderTimeoutOutput = OutputTag[OrderTimeout]("orderTimeout")

    val patternStream = CEP.pattern(orderEventStream.keyBy("orderId"), orderPayPattern)

    val complexResult = patternStream.select(orderTimeoutOutput) {
      (pattern: Map[String, Iterable[OrderEvent]], timestamp: Long) => {
        val createOrder = pattern.get("begin")
        OrderTimeout(createOrder.get.iterator.next().orderId, "timeout")
      }
    } {
      pattern: Map[String, Iterable[OrderEvent]] => {
        val payOrder = pattern.get("next")
        OrderTimeout(payOrder.get.iterator.next().orderId, "success")
      }
    }
    val timeoutResult = complexResult.getSideOutput(orderTimeoutOutput)

    complexResult.print()
    timeoutResult.print()

    env.execute

  }

}

