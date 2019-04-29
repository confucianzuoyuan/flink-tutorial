### 将事件发送到侧输出

大部分的DataStream API的算子的输出是单一输出，也就是某种数据类型的流。除了split算子，可以将一条流分成多条流，这些流的数据类型也都相同。process function的side outputs功能可以产生多条流，并且这些流的数据类型可以不一样。一个side output可以定义为OutputTag[X]对象，X是输出流的数据类型。process function可以通过Context对象发射一个事件到一个或者多个side outputs。

例子

**scala version**

```scala
object SideOutputExample {

  val output = new OutputTag[String]("side-output")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)

    val warnings = stream
      .process(new FreezingAlarm)

    warnings.print() // 打印主流
    warnings.getSideOutput(output).print() // 打印侧输出流

    env.execute()
  }

  class FreezingAlarm extends ProcessFunction[SensorReading, SensorReading] {
    override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
      if (value.temperature < 32.0) {
        ctx.output(output, "传感器ID为：" + value.id + "的传感器温度小于32度！")
      }
      out.collect(value)
    }
  }
}
```


**java version**


```java
public class SideOutputExample {

    private static OutputTag<String> output = new OutputTag<String>("side-output"){};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<SensorReading> stream = env.addSource(new SensorSource());

        SingleOutputStreamOperator<SensorReading> warnings = stream
                .process(new ProcessFunction<SensorReading, SensorReading>() {
                    @Override
                    public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
                        if (value.temperature < 32) {
                            ctx.output(output, "温度小于32度！");
                        }
                        out.collect(value);
                    }
                });

        warnings.print();
        warnings.getSideOutput(output).print();

        env.execute();
    }
}
```

