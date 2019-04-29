### 使用Process Function实现订单超时需求

```scala
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object OrderTimeoutWIthoutCep {

  case class OrderEvent(orderId: String,
                        eventType: String,
                        eventTime: String)

  def main(args: Array[String]): Unit = {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env
      .fromElements(
        OrderEvent("1", "create", "2"),
        OrderEvent("2", "create", "3"),
        OrderEvent("2", "pay", "4")
      )
      .assignAscendingTimestamps(_.eventTime.toLong * 1000L)
      .keyBy(_.orderId)
      .process(new OrderMatchFunc)

    stream.print()
    env.execute()
  }

  class OrderMatchFunc extends KeyedProcessFunction[String, OrderEvent, String] {
    lazy val orderState = getRuntimeContext.getState(
      new ValueStateDescriptor[OrderEvent]("saved order", Types.of[OrderEvent])
    )

    override def processElement(value: OrderEvent,
                                ctx: KeyedProcessFunction[String, OrderEvent, String]#Context,
                                out: Collector[String]): Unit = {
      if (value.eventType.equals("create")) {
        if (orderState.value() == null) { // 为什么要判空？因为可能出现`pay`先到的情况
          // 如果orderState为空，保存`create`事件
          orderState.update(value)
        }
      } else {
        // 保存`pay`事件
        orderState.update(value)
      }

      ctx.timerService().registerEventTimeTimer(value.eventTime.toLong * 1000 + 5000L)
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[String, OrderEvent, String]#OnTimerContext,
                         out: Collector[String]): Unit = {
      val savedOrder = orderState.value()

      if (savedOrder != null && savedOrder.eventType.equals("create")) {
        out.collect("超时订单的ID为：" + savedOrder.orderId)
      }

      orderState.clear()
    }
  }
}
```

