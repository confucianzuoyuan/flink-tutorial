import java.io.Serializable
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.slf4j.LoggerFactory

import scala.collection.Map

object OrderTimeout {

  val LOGGER = LoggerFactory.getLogger(classOf[App])

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val orderEventStream = env.fromCollection(new DataSource())

    val orderPayPattern = Pattern.begin[OrderEvent]("begin")
      .where(_.`type`.equals("create"))
      .next("next")
      .where(_.`type`.equals("pay"))
      .within(Time.seconds(1))

    val orderTiemoutOutput = OutputTag[OrderEvent]("orderTimeout")

    val patternStream = CEP.pattern(orderEventStream.keyBy("userId"), orderPayPattern)

    val complexResult = patternStream.select(orderTiemoutOutput) {
      (pattern: Map[String, Iterable[OrderEvent]], timestamp: Long) => {
        val createOrder = pattern.get("begin")
        OrderEvent("timeout", createOrder.get.iterator.next().userId)
      }
    } {
      pattern: Map[String, Iterable[OrderEvent]] => {
        val payOrder = pattern.get("next")
        OrderEvent("success", payOrder.get.iterator.next().userId)
      }
    }
    val timeoutResult = complexResult.getSideOutput(orderTiemoutOutput)

    complexResult.print()
    timeoutResult.print()

    env.execute

  }

}

class DataSource extends Iterator[OrderEvent] with Serializable {
  val atomicInteger = new AtomicInteger(0)

  val orderEventList = List(
    OrderEvent("1", "create"),
    OrderEvent("2", "create"),
    OrderEvent("2", "pay")
  )

  override def hasNext: Boolean = {
    TimeUnit.SECONDS.sleep(1)
    true
  }

  override def next(): OrderEvent = {
    orderEventList(atomicInteger.getAndIncrement() % 3)
  }
}

case class OrderEvent(userId: String, `type`: String)