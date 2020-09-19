## 你好，Flink！

让我们写一个简单的例子来获得使用DataStream API编写流处理应用程序的粗浅印象。我们将使用这个简单的示例来展示一个Flink程序的基本结构，以及介绍一些DataStream API的重要特性。我们的示例程序摄取了一条（来自多个传感器的）温度测量数据流。

首先让我们看一下表示传感器读数的数据结构：

**scala version**

```scala
case class SensorReading(id: String, timestamp: Long, temperature: Double)
```

**java version**

```java
public class SensorReading {

    public String id;
    public long timestamp;
    public double temperature;

    public SensorReading() { }

    public SensorReading(String id, long timestamp, double temperature) {
        this.id = id;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }

    public String toString() {
        return "(" + this.id + ", " + this.timestamp + ", " + this.temperature + ")";
    }
}
```

示例程序5-1将温度从华氏温度读数转换成摄氏温度读数，然后针对每一个传感器，每5秒钟计算一次平均温度纸。

**scala version**

```scala
object AverageSensorReadings {
  def main(args: Array[String]) {
    // 创建运行时环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 使用事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val sensorData: DataStream[SensorReading] = env.addSource(new SensorSource)

    val avgTemp = sensorData
      .map(r => {
        val celsius = (r.temperature - 32) * (5.0 / 9.0)
        SensorReading(r.id, r.timestamp, celsius)
      })
      .keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .apply(new TemperatureAverager)

    avgTemp.print()

    env.execute("Compute average sensor temperature")
  }
}
```

**java version**

```java
public class AverageSensorReadings {
  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    DataStream<SensorReading> sensorData = env.addSource(new SensorSource());

    DataStream<T> avgTemp = sensorData
      .map(r -> {
        Double celsius = (r.temperature - 32) * (5.0 / 9.0);
        return SensorReading(r.id, r.timestamp, celsius);
      })
      .keyBy(r -> r.id)
      .timeWindow(Time.seconds(5))
      .apply(new TemperatureAverager());

    avgTemp.print();

    env.execute("Compute average sensor temperature");
  }
}
```

你可能已经注意到Flink程序的定义和提交执行使用的就是正常的Scala或者Java的方法。大多数情况下，这些代码都写在一个静态main方法中。在我们的例子中，我们定义了AverageSensorReadings对象，然后将大多数的应用程序逻辑放在了main()中。

Flink流处理程序的结构如下：

1. 创建Flink程序执行环境。
2. 从数据源读取一条或者多条流数据
3. 使用流转换算子实现业务逻辑
4. 将计算结果输出到一个或者多个外部设备（可选）
5. 执行程序

接下来我们详细的学习一下这些部分。

