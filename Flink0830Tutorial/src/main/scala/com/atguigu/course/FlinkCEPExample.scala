package com.atguigu.course

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.Map

object FlinkCEPExample {

  case class LoginEvent(userId: String, ip: String, eventType: String, eventTime: String)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env
      .fromElements(
        LoginEvent("1", "192.168.0.1", "fail", "1"),
        LoginEvent("1", "192.168.0.2", "fail", "2"),
        LoginEvent("1", "192.168.0.3", "fail", "3"),
        LoginEvent("2", "192.168.0.4", "success", "4")
      )
      .assignAscendingTimestamps(_.eventTime.toLong * 1000)
      .keyBy(_.userId)

    val pattern = Pattern
      .begin[LoginEvent]("first").where(_.eventType == "fail")
      .next("second").where(_.eventType == "fail")
      .next("third").where(_.eventType == "fail")
      .within(Time.seconds(10))

    val patternStream = CEP.pattern(stream, pattern)

    val loginFailStream = patternStream
      .select((pattern: Map[String, Iterable[LoginEvent]]) => {
        val first = pattern.getOrElse("first", null).iterator.next()
        val second = pattern.getOrElse("second", null).iterator.next()
        val third = pattern.getOrElse("third", null).iterator.next()

        "userId是： " + first.userId + " 的用户， 10s 之内连续登录失败了三次，" + "ip地址分别是： " + first.ip + "; " + second.ip + "; " + third.ip
      })

    loginFailStream.print()

    env.execute()
  }
}
