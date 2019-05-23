import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

case class OrderEvent(orderId: String, eventType: String, eventTime: String)

case class PayEvent(orderId: String, eventType: String, eventTime: String)


object RidesAndFaresSolution {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val orders = env
      .fromCollection(List(
        OrderEvent("1", "create", "1558430842"),
        OrderEvent("2", "create", "1558430843"),
        OrderEvent("1", "pay", "1558430844"),
        OrderEvent("2", "pay", "1558430845"),
        OrderEvent("3", "create", "1558430849")
      ))
      .keyBy("orderId")

    val fares = env
        .fromCollection(List(
          PayEvent("1", "weixin", "1558430847"),
          PayEvent("2", "zhifubao", "1558430848")
        ))
      .keyBy("orderId")

    val processed = orders
      .connect(fares)
      .flatMap(new EnrichmentFunction)
      .print()

    env.execute("Join Orders with Payments (scala RichCoFlatMap)")
  }

  class EnrichmentFunction extends RichCoFlatMapFunction[OrderEvent, PayEvent, (OrderEvent, PayEvent)] {
    // keyed, managed state
    lazy val orderState: ValueState[OrderEvent] = getRuntimeContext.getState(
      new ValueStateDescriptor[OrderEvent]("saved ride", classOf[OrderEvent]))
    lazy val payState: ValueState[PayEvent] = getRuntimeContext.getState(
      new ValueStateDescriptor[PayEvent]("saved fare", classOf[PayEvent]))

    override def flatMap1(order: OrderEvent, out: Collector[(OrderEvent, PayEvent)]): Unit = {
      val pay = payState.value
      if (pay != null) {
        payState.clear()
        out.collect((order, pay))
      }
      else {
        orderState.update(order)
      }
    }

    override def flatMap2(pay: PayEvent, out: Collector[(OrderEvent, PayEvent)]): Unit = {
      val order = orderState.value
      if (order != null) {
        orderState.clear()
        out.collect((order, pay))
      }
      else {
        payState.update(pay)
      }
    }
  }

}
