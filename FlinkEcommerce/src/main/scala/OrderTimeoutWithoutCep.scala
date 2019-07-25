import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

object OrderTimeoutWithoutCep {

  case class OrderEvents(orderId: String, eventType: String, eventTime: String)

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val orderEventsStream = env.fromCollection(List(
      OrderEvents("1", "create", "1558430842"),
      OrderEvents("2", "create", "1558430843"),
      OrderEvents("2", "pay", "1558430844"),
      OrderEvents("3", "pay", "1558430942"),
      OrderEvents("4", "pay", "1558430943")
    )).assignAscendingTimestamps(_.eventTime.toLong * 1000)

    val orders = orderEventsStream
      .keyBy(_.orderId)
      .process(new OrderMatchFunction())
      .print()

    env.execute
  }

  class OrderMatchFunction extends KeyedProcessFunction[String, OrderEvents, OrderEvents] {
    // keyed, managed state
    // holds an END event if the order has ended, otherwise a START event
    lazy val orderState: ValueState[OrderEvents] = getRuntimeContext.getState(
      new ValueStateDescriptor[OrderEvents]("saved order", classOf[OrderEvents]))

    override def processElement(order: OrderEvents,
                                context: KeyedProcessFunction[String, OrderEvents, OrderEvents]#Context,
                                out: Collector[OrderEvents]): Unit = {
      val timerService = context.timerService

      if (order.eventType == "create") {
        // the matching END might have arrived first; don't overwrite it
        if (orderState.value() == null) {
          orderState.update(order)
        }
      }
      else {
        orderState.update(order)
      }

      timerService.registerEventTimeTimer(order.eventTime.toLong * 1000 + 5 * 1000)
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[String, OrderEvents, OrderEvents]#OnTimerContext,
                         out: Collector[OrderEvents]): Unit = {
      println(timestamp)
      val savedOrder = orderState.value

      if (savedOrder != null && (savedOrder.eventType == "create")) {
        out.collect(savedOrder)
      }

      orderState.clear()
    }
  }

}
