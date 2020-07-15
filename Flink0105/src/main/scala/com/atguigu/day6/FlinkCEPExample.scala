package com.atguigu.day6

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object FlinkCEPExample {

  case class LoginEvent(userId: String, eventType: String, ipAddr: String, eventTime: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env
      .fromElements(
        LoginEvent("user_1", "fail", "0.0.0.1", 1000L),
        LoginEvent("user_1", "fail", "0.0.0.2", 2000L),
        LoginEvent("user_1", "fail", "0.0.0.3", 3000L),
        LoginEvent("user_2", "success", "0.0.0.1", 4000L)
      )
      .assignAscendingTimestamps(_.eventTime)
      .keyBy(_.userId)

    // 声明一个需要检测的规则
    // 10s之内连续3次登录失败
    val pattern = Pattern
      .begin[LoginEvent]("first") // 第一个事件命名为first
      .where(_.eventType.equals("fail")) // 第一个事件需要满足的条件
      .next("second") // 第二个事件命名为second，next表示第二个事件和第一个事件必须紧挨着
      .where(_.eventType.equals("fail")) // 第二个事件需要满足的条件
      .next("third")
      .where(_.eventType.equals("fail"))
      .within(Time.seconds(10)) // 要求三个事件必须在10s之内连续发生，从begin开始

    // 第一个参数是待匹配的流，第二个参数是匹配规则
    val patternStream = CEP.pattern(stream, pattern)

    // 使用select方法，将匹配到的事件找到
    patternStream
      .select((pattern: scala.collection.Map[String, Iterable[LoginEvent]]) => {
        val first = pattern("first").iterator.next() // first是之前我们为规则中的第一个事件取得名字，匹配到的事件在迭代器中
        val second = pattern("second").iterator.next()
        val third = pattern("third").iterator.next()

        "用户 " + first.userId + " 分别在ip: " + first.ipAddr + " ; " + second.ipAddr + " ; " + third.ipAddr + " 登录失败！"
      })
      .print()

    env.execute()
  }
}