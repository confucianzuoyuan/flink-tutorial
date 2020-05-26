package com.atguigu.day7

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object OrderTimeoutDetect {

  case class OrderEvent(orderId: String, eventType: String, eventTime: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env
      .fromElements(
        OrderEvent("order_1", "create", 2000L),
        OrderEvent("order_2", "create", 3000L),
        OrderEvent("order_2", "pay", 4000L)
      )
      .assignAscendingTimestamps(_.eventTime)
      .keyBy(_.orderId)

    // 定义的规则
    val pattern = Pattern
      .begin[OrderEvent]("first")
      .where(_.eventType.equals("create"))
      .next("second")
      .where(_.eventType.equals("pay"))
      .within(Time.seconds(5))

    val patternStream = CEP.pattern(stream, pattern)

    // 用来输出超时订单的信息
    // 超时订单的意思是只有create事件，没有pay事件
    val orderTimeoutOutputTag = new OutputTag[String]("timeout")

    // 这个匿名函数用来处理超时的检测
    val timeoutFunc = (pattern: scala.collection.Map[String, Iterable[OrderEvent]], ts: Long, out: Collector[String]) => {
      val orderCreate = pattern("first").iterator.next()
      out.collect("----------------")
      out.collect("在 " + ts + " ms之前没有支付！超时了！超时订单的ID为 " + orderCreate.orderId)
      out.collect("----------------")
    }

    // 这个匿名函数用来处理支付成功的检测
    val selectFunc = (pattern: scala.collection.Map[String, Iterable[OrderEvent]], out: Collector[String]) => {
      val orderPay = pattern("second").iterator.next()
      out.collect("================")
      out.collect("订单ID为 " + orderPay.orderId + " 支付成功！")
      out.collect("================")
    }

    val detectStream = patternStream
      // flatSelect方法接受柯里化参数
      // 第一个参数：检测出的超时信息发送到的侧输出标签
      // 第二个参数：用来处理超时信息的函数
      // 第三个参数：用来处理create和pay匹配成功的信息
      .flatSelect(orderTimeoutOutputTag)(timeoutFunc)(selectFunc)

    // 打印匹配成功的信息
    detectStream.print()
    // 打印超时信息
    detectStream.getSideOutput(orderTimeoutOutputTag).print()

    env.execute()
  }
}