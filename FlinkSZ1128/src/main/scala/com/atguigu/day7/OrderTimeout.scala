package com.atguigu.day7

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.Map

object OrderTimeout {
  case class OrderEvent(orderId: String, eventType: String, eventTime: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env
      .fromElements(
        OrderEvent("order_1", "create", 2000L),
        OrderEvent("order_2", "create", 3000L),
        OrderEvent("order_2", "pay", 4000L)
      )
      .assignAscendingTimestamps(_.eventTime)
      .keyBy(_.orderId)

    val pattern = Pattern
      .begin[OrderEvent]("create").where(_.eventType.equals("create"))
      .next("pay").where(_.eventType.equals("pay"))
      .within(Time.seconds(5))

    val patternedStream = CEP.pattern(stream, pattern)

    // 用来输出超时订单的侧输出标签
    val orderTimeoutOutput = new OutputTag[String]("timeout")

    // 用来处理超时订单的函数
    val timeoutFunc = (map: Map[String, Iterable[OrderEvent]], ts: Long, out: Collector[String]) => {
      println("ts" + ts) // 2s + 5s
      val orderStart = map("create").head
      // 将报警信息发送到侧输出流去
      out.collect(orderStart.orderId + "没有支付！")
    }

    val selectFunc = (map: Map[String, Iterable[OrderEvent]], out: Collector[String]) => {
      val order = map("pay").head
      out.collect(order.orderId + "已经支付！")
    }

    val outputStream = patternedStream
      // 第一个参数：用来输出超时事件的侧输出标签
      // 第二个参数：用来输出超时事件的函数
      // 第三个参数：用来输出没有超时的事件的函数
      .flatSelect(orderTimeoutOutput)(timeoutFunc)(selectFunc)

    outputStream.print()
    outputStream.getSideOutput(new OutputTag[String]("timeout")).print()

    env.execute()
  }
}