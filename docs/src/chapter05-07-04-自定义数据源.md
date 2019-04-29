### 自定义数据源

**scala version**

```scala
import java.util.Calendar

import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.util.Random

// 泛型是`SensorReading`，表明产生的流中的事件的类型是`SensorReading`
class SensorSource extends RichParallelSourceFunction[SensorReading] {
  // 表示数据源是否正常运行
  var running: Boolean = true

  // 上下文参数用来发出数据
  override def run(ctx: SourceContext[SensorReading]): Unit = {
    val rand = new Random

    var curFTemp = (1 to 10).map(
      // 使用高斯噪声产生随机温度值
      i => ("sensor_" + i, (rand.nextGaussian() * 20))
    )

    // 产生无限数据流
    while (running) {
      curFTemp = curFTemp.map(
        t => (t._1, t._2 + (rand.nextGaussian() * 0.5))
      )

      // 产生ms为单位的时间戳
      val curTime = Calendar.getInstance.getTimeInMillis

      // 使用ctx参数的collect方法发射传感器数据
      curFTemp.foreach(t => ctx.collect(SensorReading(t._1, curTime, t._2)))

      // 每隔100ms发送一条传感器数据
      Thread.sleep(1000)
    }
  }

  // 定义当取消flink任务时，需要关闭数据源
  override def cancel(): Unit = running = false
}
```

使用方法

```scala
val sensorData = env.addSource(new SensorSource)
```

**java version**

```java
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Calendar;
import java.util.Random;

public class SensorSource extends RichParallelSourceFunction<SensorReading> {

    private boolean running = true;

    @Override
    public void run(SourceContext<SensorReading> srcCtx) throws Exception {

        Random rand = new Random();

        String[] sensorIds = new String[10];
        double[] curFTemp = new double[10];
        for (int i = 0; i < 10; i++) {
            sensorIds[i] = "sensor_" + i;
            curFTemp[i] = 65 + (rand.nextGaussian() * 20);
        }

        while (running) {
            long curTime = Calendar.getInstance().getTimeInMillis();
            for (int i = 0; i < 10; i++) {
                curFTemp[i] += rand.nextGaussian() * 0.5;
                srcCtx.collect(new SensorReading(sensorIds[i], curTime, curFTemp[i]));
            }

            Thread.sleep(100);
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }
}
```

使用方法

```java
// 摄入数据流
DataStream<SensorReading> sensorData = env.addSource(new SensorSource());
```