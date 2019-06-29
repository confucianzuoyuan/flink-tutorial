import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

case class LoginEvent(userId: Long,
                      ip: String,
                      eventType: String,
                      eventTime: Long)

object LoginFail {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment
      .getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val loginEventStream = env
      .fromCollection(List(
        LoginEvent(1, "192.168.0.1", "fail", 1558430842),
        LoginEvent(1, "192.168.0.2", "fail", 1558430843),
        LoginEvent(1, "192.168.0.3", "fail", 1558430844),
        LoginEvent(2, "192.168.10.10", "success", 1558430845)
      ))
      .assignAscendingTimestamps(_.eventTime * 1000)
      .keyBy(_.userId)
      .process(new MatchFunction)
      .print()

    env.execute
  }
}

class MatchFunction extends KeyedProcessFunction[Long, LoginEvent, LoginEvent] {
  lazy val loginState: ListState[LoginEvent] = getRuntimeContext.getListState(
    new ListStateDescriptor[LoginEvent]("save login", classOf[LoginEvent])
  )

  override def processElement(login: LoginEvent,
                              context: KeyedProcessFunction[Long,
                                LoginEvent, LoginEvent]#Context,
                              out: Collector[LoginEvent]): Unit = {
    if (login.eventType == "fail") {
      loginState.add(login)
    }

    context.timerService.registerEventTimeTimer(login.eventTime + 2000)
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Long,
                         LoginEvent, LoginEvent]#OnTimerContext,
                       out: Collector[LoginEvent]): Unit = {
    val allLogins: ListBuffer[LoginEvent] = ListBuffer()
    import scala.collection.JavaConversions._
    for (login <- loginState.get) {
      allLogins += login
    }
    loginState.clear()

    if (allLogins.length > 1) {
      out.collect(allLogins.head)
    }
  }
}