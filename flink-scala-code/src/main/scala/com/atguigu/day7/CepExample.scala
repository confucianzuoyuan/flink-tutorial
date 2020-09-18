package com.atguigu.day7

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object CepExample {

  case class LoginEvent(userId: String, ip: String, eventType: String, eventTime: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env
      .fromElements(
        LoginEvent("user_1", "192.168.0.1", "fail", 2000L),
        LoginEvent("user_1", "192.168.0.2", "fail", 3000L),
        LoginEvent("user_1", "192.168.0.3", "fail", 4000L),
        LoginEvent("user_2", "192.168.10.10", "success", 5000L)
      )
      .assignAscendingTimestamps(_.eventTime)
      .keyBy(r => r.userId)

    val pattern = Pattern
      .begin[LoginEvent]("first")
      .where(r => r.eventType.equals("fail"))
      .next("second")
      .where(r => r.eventType.equals("fail"))
      .next("third")
      .where(r => r.eventType.equals("fail"))
      .within(Time.seconds(5))

    val pattern1 = Pattern
      .begin[LoginEvent]("first")
      .times(3)
      .where(r => r.eventType.equals("fail"))
      .within(Time.seconds(5))

    val patternedStream = CEP.pattern(stream, pattern)

    patternedStream
      .select((pattern: scala.collection.Map[String, Iterable[LoginEvent]]) => {
        val first = pattern("first").iterator.next()
        val second = pattern("second").iterator.next()
        val third = pattern("third").iterator.next()

        (first.userId, first.ip, second.ip, third.ip)
      })
      .print()

    val patternedStream1 = CEP.pattern(stream, pattern1)

    patternedStream1
      .select((pattern: scala.collection.Map[String, Iterable[LoginEvent]]) => {
        val first = pattern("first").iterator
        for (e <- first) {
          println(e)
        }
        ("cep", "done")
      })
      .print()

    env.execute()
  }
}