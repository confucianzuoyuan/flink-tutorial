import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

object BloomFilterUV {
  def main(args: Array[String]): Unit = {
    val resourcesPath = getClass.getResource("/UserBehavior.csv")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val stream = env
      .readTextFile(resourcesPath.getPath)
      .map(line => {
        val linearray = line.split(",")
        UserBehavior(linearray(0).toLong, linearray(1).toLong, linearray(2).toInt, linearray(3), linearray(4).toLong * 1000)
      })
      .filter(_.behavior=="pv")
      .assignAscendingTimestamps(_.timestamp)
      .timeWindowAll(Time.seconds(60 * 60))
      .trigger(new MyTrigger)
      .process(new MyReduceProcessFunction)

//    stream.print()

    env.execute

  }

  class MyReduceProcessFunction extends ProcessAllWindowFunction[UserBehavior, Long, TimeWindow] {

    lazy val jedis = new Jedis("localhost", 6379)

    override def process(context: Context,
                         vals: Iterable[UserBehavior],
                         out: Collector[Long]): Unit = {
      val bloom = new Bloom()
      val userId = vals.iterator.next.userId.toString
      val offset = bloom.hash(userId, 61)
      val key = context.window.getEnd.toString
      val exist = jedis.getbit(key, offset)
      if (!exist) {
        jedis.setbit(key, offset, true)
        if (jedis.hget("count", key) == null) {
          jedis.hset("count", key, "1")
        } else {
          val count = jedis.hget("count", key).toLong
          jedis.hset("count", key, (count+1).toString)
        }
      }
//      out.collect(1)
    }
  }

  class Bloom extends Serializable {
    //总的bitmap大小  64M
    private val cap = 1 << 29
    /*
     * 不同哈希函数的种子，一般取质数
     * seeds数组共有8个值，则代表采用8种不同的哈希函数
     */
    private val seeds = Array[Int](3, 5, 7, 11, 13, 31, 37, 61)

    def hash(value: String, seed: Int) = {
      var result = 0
      val length = value.length
      var i = 0
      while ( {
        i < length
      }) {
        result = seed * result + value.charAt(i)

        {
          i += 1; i - 1
        }
      }
      (cap - 1) & result
    }
  }



  class MyTrigger extends Trigger[UserBehavior, TimeWindow] {

    override def onElement(element: UserBehavior,
                           timestamp: Long,
                           window: TimeWindow,
                           ctx: Trigger.TriggerContext): TriggerResult = {
      TriggerResult.FIRE_AND_PURGE
    }

    override def onEventTime(timestamp: Long,
                             window: TimeWindow,
                             ctx: Trigger.TriggerContext): TriggerResult = {
      if (ctx.getCurrentWatermark >= window.getEnd) {
        val jedis = new Jedis("localhost", 6379)
        val key = window.getEnd.toString
        TriggerResult.FIRE_AND_PURGE
        println(key, jedis.hget("count", key))
      }
      TriggerResult.CONTINUE
    }

    override def onProcessingTime(time: Long,
                                  window: TimeWindow,
                                  ctx: Trigger.TriggerContext): TriggerResult = {
      TriggerResult.CONTINUE
    }

    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
  }

}