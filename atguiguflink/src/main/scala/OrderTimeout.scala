import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.slf4j.LoggerFactory
import org.apache.flink.streaming.api.TimeCharacteristic

import scala.collection.Map

object OrderTimeout {

  val LOGGER = LoggerFactory.getLogger(classOf[App])

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
      .where(_.`type`.equals("create"))
      .next("next")
      .where(_.`type`.equals("pay"))
      .within(Time.seconds(5))

    val orderTimeoutOutput = OutputTag[(String, String)]("orderTimeout")

    val patternStream = CEP.pattern(orderEventStream.keyBy("userId"), orderPayPattern)

    val complexResult = patternStream.select(orderTimeoutOutput) {
      (pattern: Map[String, Iterable[OrderEvent]], timestamp: Long) => {
        val createOrder = pattern.get("begin")
        ("timeout", createOrder.get.iterator.next().userId)
      }
    } {
      pattern: Map[String, Iterable[OrderEvent]] => {
        val payOrder = pattern.get("next")
        ("success", payOrder.get.iterator.next().userId)
      }
    }
    val timeoutResult = complexResult.getSideOutput(orderTimeoutOutput)

    complexResult.print()
    timeoutResult.print()

    env.execute

  }

}

case class OrderEvent(userId: String, `type`: String, eventTime: String)