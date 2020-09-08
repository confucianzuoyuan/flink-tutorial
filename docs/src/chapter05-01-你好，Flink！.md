## 你好，Flink！

让我们写一个简单的例子来获得使用DataStream API编写流处理应用程序的粗浅印象。我们将使用这个简单的示例来展示一个Flink程序的基本结构，以及介绍一些DataStream API的重要特性。我们的示例程序摄取了一条（来自多个传感器的）温度测量数据流。

首先让我们看一下表示传感器读数的数据结构：

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

```java
// Scala object that defines
// the DataStream program in the main() method.
object AverageSensorReadings {
  // main() defines and executes the DataStream program
  def main(args: Array[String]) {
    // set up the streaming execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment
    // use event time for the application
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // create a DataStream[SensorReading] from a stream source
    val sensorData: DataStream[SensorReading] = env
      // ingest sensor readings with a SensorSource SourceFunction
      .addSource(new SensorSource)
      // assign timestamps and watermarks (required for event time)
    val avgTemp: DataStream[SensorReading] = sensorData
      // convert Fahrenheit to Celsius with an inline lambda function
      .map( r => {
        val celsius = (r.temperature - 32) * (5.0 / 9.0)
        SensorReading(r.id, r.timestamp, celsius)
      })
      // organize readings by sensor id
      .keyBy(_.id)
      // group readings in 5 second tumbling windows
      .timeWindow(Time.seconds(5))
      // compute average temperature using a user-defined function
      .apply(new TemperatureAverager)
      // print result stream to standard out
      avgTemp.print()
    // execute application
    env.execute("Compute average sensor temperature")
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

