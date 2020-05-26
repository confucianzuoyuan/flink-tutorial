### 自定义数据源

```scala
import java.util.Calendar

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import scala.util.Random

// 传感器id，时间戳，温度
case class SensorReading(id: String, timestamp: Long, temperature: Double)

// 需要extends RichParallelSourceFunction, 泛型为SensorReading
class SensorSource
  extends RichParallelSourceFunction[SensorReading] {

  // flag indicating whether source is still running.
  // flag: 表示数据源是否还在正常运行
  var running: Boolean = true

  // run()函数连续的发送SensorReading数据，使用SourceContext
  // 需要override
  override def run(srcCtx: SourceContext[SensorReading]): Unit = {

    // initialize random number generator
    // 初始化随机数发生器
    val rand = new Random()
    // look up index of this parallel task
    // 查找当前运行时上下文的任务的索引
    val taskIdx = this.getRuntimeContext.getIndexOfThisSubtask

    // initialize sensor ids and temperatures
    // 初始化10个(温度传感器的id, 温度值)元组
    var curFTemp = (1 to 10).map {
      // nextGaussian产生高斯随机数
      i => ("sensor_" + (taskIdx * 10 + i), 65 + (rand.nextGaussian() * 20))
    }

    // emit data until being canceled
    // 无限循环，产生数据流
    while (running) {

      // update temperature
      // 更新温度
      curFTemp = curFTemp.map(t => (t._1, t._2 + (rand.nextGaussian() * 0.5)) )
      // get current time
      // 获取当前时间戳
      val curTime = Calendar.getInstance.getTimeInMillis

      // emit new SensorReading
      // 发射新的传感器数据, 注意这里srcCtx.collect
      curFTemp.foreach(t => srcCtx.collect(SensorReading(t._1, curTime, t._2)))

      // wait for 100 ms
      Thread.sleep(100)
    }

  }

  // override cancel函数
  override def cancel(): Unit = {
    running = false
  }

}
```

使用方法

```scala
// ingest sensor stream
val sensorData: DataStream[SensorReading] = env
  // SensorSource generates random temperature readings
  .addSource(new SensorSource)
```

>注意，在我们本教程中，我们一直会使用这个自定义的数据源。

