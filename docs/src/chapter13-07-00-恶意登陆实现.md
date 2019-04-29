## 恶意登陆实现

```scala
import com.atguigu.FlinkCepExample.LoginEvent
import org.apache.flink.api.common.state.{ListStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object LoginFailWithoutCEP {
  def main(args: Array[String]): Unit = {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env
      .fromElements(
        LoginEvent("1", "0.0.0.0", "fail", "1"),
        LoginEvent("1", "0.0.0.0", "success", "2"),
        LoginEvent("1", "0.0.0.0", "fail", "3"),
        LoginEvent("1", "0.0.0.0", "fail", "4")
      )
      .assignAscendingTimestamps(_.ts.toLong * 1000)
      .keyBy(_.userId)
      .process(new MatchFunction)

    stream.print()
    env.execute()
  }

  class MatchFunction extends KeyedProcessFunction[String, LoginEvent, String] {

    lazy val loginState = getRuntimeContext.getListState(
      new ListStateDescriptor[LoginEvent]("login-fail", Types.of[LoginEvent])
    )

    lazy val timestamp = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("ts", Types.of[Long])
    )

    override def processElement(
      value: LoginEvent,
      ctx: KeyedProcessFunction[String, LoginEvent, String]#Context,
      out: Collector[String]
    ): Unit = {
      if (value.loginStatus == "fail") {
        loginState.add(value)
        if (!timestamp.value()) {
          timestamp.update(value.ts.toLong * 1000 + 5000L)
          ctx
            .timerService()
            .registerEventTimeTimer(value.ts.toLong * 1000 + 5000L)
        }
      }

      if (value.loginStatus == "success") {
        loginState.clear()
        ctx
          .timerService()
          .deleteEventTimeTimer(timestamp.value())
      }
    }

    override def onTimer(
      ts: Long,
      ctx: KeyedProcessFunction[String, LoginEvent, String]#OnTimerContext,
      out: Collector[String]
    ): Unit = {
      val allLogins = ListBuffer[LoginEvent]()
      import scala.collection.JavaConversions._
      for (login <- loginState.get) {
        allLogins += login
      }
      loginState.clear()

      if (allLogins.length > 1) {
        out.collect("5s以内连续两次登陆失败")
      }
    }
  }
}
```

