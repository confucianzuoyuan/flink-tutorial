import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

case class OrderEvent1(orderId: String,
                      eventType: String,
                      eventTime: String)

object OrderTimeoutWithoutCep {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val orderEventsStream = env.fromCollection(List(
      OrderEvent1("1", "create", "1558430842"),
      OrderEvent1("2", "create", "1558430843"),
      OrderEvent1("2", "pay", "1558430844"),
      OrderEvent1("3", "pay", "1558430942"),
      OrderEvent1("4", "pay", "1558430943")
    )).assignAscendingTimestamps(_.eventTime.toLong * 1000)

    val orders = orderEventsStream
      .keyBy(_.orderId)
      .process(new OrderMatchFunction)
      .print()

    env.execute
  }

  class OrderMatchFunction extends KeyedProcessFunction[String,
    OrderEvent1, OrderEvent1] {
    lazy val orderState: ValueState[OrderEvent1] = getRuntimeContext
      .getState(new ValueStateDescriptor[OrderEvent1]("saved order",
        classOf[OrderEvent1]))

    override def processElement(order: OrderEvent1,
                                context: KeyedProcessFunction[
                                  String, OrderEvent1, OrderEvent1]#Context,
                                out: Collector[OrderEvent1]): Unit = {
//      val timerService = context.timerService

      if (order.eventType == "create") {
        if (orderState.value() == null) {
          orderState.update(order)
        }
      } else {
        orderState.update(order)
      }

      context.timerService.registerEventTimeTimer(
        order.eventTime.toLong * 1000 + 5 * 1000
      )
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[
                           String, OrderEvent1, OrderEvent1]#OnTimerContext,
                         out: Collector[OrderEvent1]): Unit = {
      val savedOrder = orderState.value()

      if (savedOrder != null &&
        (savedOrder.eventType == "create")) {
        out.collect(savedOrder)
      }

      orderState.clear()
    }
  }
}