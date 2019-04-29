package com.atguigu.project.order

import com.atguigu.project.util.{OrderEvent, PayEvent}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object RealTimeOrderConfirm {
  val unmatchedOrders = new OutputTag[OrderEvent]("unmatchedOrders"){}
  val unmatchedPays = new OutputTag[PayEvent]("unmatchedPays"){}

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val orders = env
      .fromCollection(List(
        OrderEvent("1", "create", "1558430842"),
        OrderEvent("2", "create", "1558430843"),
        OrderEvent("1", "pay", "1558430844"),
        OrderEvent("2", "pay", "1558430845"),
        OrderEvent("3", "create", "1558430849"),
        OrderEvent("3", "pay", "1558430849")
      ))
      .assignAscendingTimestamps(_.eventTime.toLong * 1000)
      .keyBy("orderId")

    val pays = env
      .fromCollection(List(
        PayEvent("1", "weixin", "1558430847"),
        PayEvent("2", "zhifubao", "1558430848"),
        PayEvent("4", "zhifubao", "1558430850")
      ))
      .assignAscendingTimestamps(_.eventTime.toLong * 1000)
      .keyBy("orderId")

    val processed = orders
      .connect(pays)
      .process(new EnrichmentFunction)

    processed.getSideOutput[PayEvent](unmatchedPays).print()
    processed.getSideOutput[OrderEvent](unmatchedOrders).print()

    env.execute
  }

  class EnrichmentFunction extends CoProcessFunction[OrderEvent, PayEvent, (OrderEvent, PayEvent)] {
    lazy val orderState: ValueState[OrderEvent] = getRuntimeContext.getState(
      new ValueStateDescriptor[OrderEvent]("saved order", Types.of[OrderEvent]))

    lazy val payState: ValueState[PayEvent] = getRuntimeContext.getState(
      new ValueStateDescriptor[PayEvent]("saved pay", Types.of[PayEvent]))

    override def processElement1(order: OrderEvent, context: CoProcessFunction[OrderEvent, PayEvent, (OrderEvent, PayEvent)]#Context, out: Collector[(OrderEvent, PayEvent)]): Unit = {
      val pay = payState.value()

      if (pay != null) {
        payState.clear()
        out.collect((order, pay))
      } else {
        orderState.update(order)
        // as soon as the watermark arrives,
        // we can stop waiting for the corresponding pay
        context.timerService.registerEventTimeTimer(order.eventTime.toLong * 1000)
      }
    }

    override def processElement2(pay: PayEvent, context: CoProcessFunction[OrderEvent, PayEvent, (OrderEvent, PayEvent)]#Context, out: Collector[(OrderEvent, PayEvent)]): Unit = {
      val order = orderState.value()

      if (order != null) {
        orderState.clear()
        out.collect((order, pay))
      } else {
        payState.update(pay)
        context.timerService.registerEventTimeTimer(pay.eventTime.toLong * 1000)
      }
    }

    override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, PayEvent, (OrderEvent, PayEvent)]#OnTimerContext, out: Collector[(OrderEvent, PayEvent)]): Unit = {
      if (payState.value != null) {
        ctx.output(unmatchedPays, payState.value)
        payState.clear()
      }

      if (orderState.value != null) {
        ctx.output(unmatchedOrders, orderState.value)
        orderState.clear()
      }
    }
  }
}