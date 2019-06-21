import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

case class OrderEvents(orderId: String, eventType: String, eventTime: String)

object OrderTimeoutWithoutCep {

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

    val longRides = orderEventsStream
      .keyBy(_.orderId)
      .process(new OrderMatchFunction())
      .print()

    env.execute
  }

  class OrderMatchFunction extends KeyedProcessFunction[String, OrderEvents, OrderEvents] {
    // keyed, managed state
    // holds an END event if the ride has ended, otherwise a START event
    lazy val orderState: ValueState[OrderEvents] = getRuntimeContext.getState(
      new ValueStateDescriptor[OrderEvents]("saved ride", classOf[OrderEvents]))

    override def processElement(ride: OrderEvents,
                                context: KeyedProcessFunction[String, OrderEvents, OrderEvents]#Context,
                                out: Collector[OrderEvents]): Unit = {
      val timerService = context.timerService

      if (ride.eventType == "create") {
        // the matching END might have arrived first; don't overwrite it
        if (orderState.value() == null) {
          orderState.update(ride)
        }
      }
      else {
        orderState.update(ride)
      }

      timerService.registerEventTimeTimer(ride.eventTime.toLong * 1000 + 5 * 1000)
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[String, OrderEvents, OrderEvents]#OnTimerContext,
                         out: Collector[OrderEvents]): Unit = {
      println(timestamp)
      val savedRide = orderState.value

      if (savedRide != null && (savedRide.eventType == "create")) {
        out.collect(savedRide)
      }

      orderState.clear()
    }
  }

}
