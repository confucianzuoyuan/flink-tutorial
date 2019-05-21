import atguigu.entity.{LoginEvent, LoginWarning}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.Map

object ScalaFlinkLoginFail {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val loginEventStream = env.fromCollection(List(
      new LoginEvent("1", "192.168.0.1", "fail"),
      new LoginEvent("1", "192.168.0.2", "fail"),
      new LoginEvent("1", "192.168.0.3", "fail"),
      new LoginEvent("2", "192.168.10.10", "success")
    ))

    val loginFailPattern = Pattern.begin[LoginEvent]("begin")
      .where(_.getType.equals("fail"))
      .next("next")
      .where(_.getType.equals("fail"))
      .within(Time.seconds(1))

    val patternStream = CEP.pattern(loginEventStream, loginFailPattern)

    val loginFailDataStream = patternStream
      .select((pattern: Map[String, Iterable[LoginEvent]]) => {
        val first = pattern.getOrElse("begin", null).iterator.next()
        val second = pattern.getOrElse("next", null).iterator.next()

        new LoginWarning(second.getUserId, second.getIp, second.getType)
      })

    loginFailDataStream.print

    env.execute
  }

}