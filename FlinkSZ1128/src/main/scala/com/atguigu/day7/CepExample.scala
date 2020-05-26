package com.atguigu.day7

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

import scala.collection.Map

// 检测连续三次登录失败的事件
object CepExample {

  case class LoginEvent(userId: String, ip: String, eventType: String, eventTime: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env
      .fromElements(
        LoginEvent("user_1", "0.0.0.0", "fail", 1000L),
        LoginEvent("user_1", "0.0.0.1", "fail", 2000L),
        LoginEvent("user_1", "0.0.0.2", "fail", 3000L),
        LoginEvent("user_2", "0.0.0.0", "success", 4000L)
      )
      .assignAscendingTimestamps(_.eventTime)
      .keyBy(_.userId)

    // 定义需要匹配的模板
    val pattern = Pattern
      .begin[LoginEvent]("first").where(_.eventType.equals("fail"))
      .next("second").where(_.eventType.equals("fail"))
      .next("third").where(_.eventType.equals("fail"))
      .within(Time.seconds(10)) // 10s之内连续三次登录失败

    // 第一个参数：需要匹配的流，第二个参数：模板
    val patternedStream = CEP.pattern(stream, pattern)

    patternedStream
      .select(func)
      .print()

    env.execute()
  }

  // 注意匿名函数的类型
  val func = (pattern: Map[String, Iterable[LoginEvent]]) => {
    val first = pattern.getOrElse("first", null).iterator.next()
    val second = pattern.getOrElse("second", null).iterator.next()
    val third = pattern.getOrElse("third", null).iterator.next()

    first.userId + "连续三次登录失败！"
  }
}