### UDF

用户定义函数（User-defined Functions，UDF）是一个重要的特性，因为它们显著地扩展了查询（Query）的表达能力。一些系统内置函数无法解决的需求，我们可以用UDF来自定义实现。

#### 注册用户自定义函数UDF

在大多数情况下，用户定义的函数必须先注册，然后才能在查询中使用。不需要专门为Scala 的Table API注册函数。

函数通过调用registerFunction()方法在TableEnvironment中注册。当用户定义的函数被注册时，它被插入到TableEnvironment的函数目录中，这样Table API或SQL解析器就可以识别并正确地解释它。

#### 标量函数（Scalar Functions）

用户定义的标量函数，可以将0、1或多个标量值，映射到新的标量值。

为了定义标量函数，必须在org.apache.flink.table.functions中扩展基类Scalar Function，并实现（一个或多个）求值（evaluation，eval）方法。标量函数的行为由求值方法决定，求值方法必须公开声明并命名为eval（直接def声明，没有override）。求值方法的参数类型和返回类型，确定了标量函数的参数和返回类型。

在下面的代码中，我们定义自己的HashCode函数，在TableEnvironment中注册它，并在查询中调用它。

```scala
// 自定义一个标量函数
  class HashCodeFunction extends ScalarFunction {

    private var factor: Int = 0

    override def open(context: FunctionContext): Unit = {
      // 获取参数 "hashcode_factor"
      // 如果不存在，则使用默认值 "12"
      factor = context.getJobParameter("hashcode_factor", "12").toInt
    }

    def eval(s: String): Int = {
      s.hashCode * factor
    }
  }
```

主函数中调用，计算sensor id的哈希值（前面部分照抄，流环境、表环境、读取source、建表）：

```scala
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.functions.{FunctionContext, ScalarFunction}
import org.apache.flink.types.Row

object ScalarFunctionExample {
  def main(args: Array[String]): Unit = {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)

    val settings = EnvironmentSettings
        .newInstance()
        .inStreamingMode()
        .build()

    val tEnv = StreamTableEnvironment.create(env, settings)

    tEnv.getConfig.addJobParameter("hashcode_factor", "31")

    tEnv.createTemporaryView("sensor", stream)

    // 在 Table API 里不经注册直接“内联”调用函数
    tEnv.from("sensor").select(call(classOf[HashCodeFunction], $"id"))

    // sql 写法
    // 注册函数
    tEnv.createTemporarySystemFunction("hashCode", classOf[HashCodeFunction])

    // 在 Table API 里调用注册好的函数
    tEnv.from("sensor").select(call("hashCode", $"id"))

    tEnv
        .sqlQuery("SELECT id, hashCode(id) FROM sensor")
        .toAppendStream[Row]
        .print()

    env.execute()
  }

  class HashCodeFunction extends ScalarFunction {

    private var factor: Int = 0

    override def open(context: FunctionContext): Unit = {
      // 获取参数 "hashcode_factor"
      // 如果不存在，则使用默认值 "12"
      factor = context.getJobParameter("hashcode_factor", "12").toInt
    }

    def eval(s: String): Int = {
      s.hashCode * factor
    }
  }
}
```

**java version**

```java
public class ScalarFunctionExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());

        Table table = tEnv
                .fromDataStream(
                        stream,
                        $("id"),
                        $("timestamp").as("ts"),
                        $("temperature"),
                        $("pt").proctime());

        tEnv.createTemporaryView(
                "sensor",
                stream,
                $("id"),
                $("timestamp").as("ts"),
                $("temperature"),
                $("pt").proctime());

        tEnv.getConfig().addJobParameter("hashcode_factor", "31");

        // table api
        Table tableResult = tEnv.from("sensor").select(call(HashCodeFunction.class, $("id")));
//        tEnv.toAppendStream(tableResult, Row.class).print();

        // sql写法
        // 注册udf函数
        tEnv.createTemporarySystemFunction("hashCode", HashCodeFunction.class);

        Table sqlResult = tEnv.sqlQuery("SELECT id, hashCode(id) FROM sensor");
        tEnv.toAppendStream(sqlResult, Row.class).print();


        env.execute();
    }

    public static class HashCodeFunction extends ScalarFunction {
        private Integer factor = 0;

        @Override
        public void open(FunctionContext context) throws Exception {
            super.open(context);
            factor = Integer.parseInt(context.getJobParameter("hashcode_factor", "12"));
        }

        public Integer eval(String s) {
            return s.hashCode() * factor;
        }
    }
}
```

#### 表函数（Table Functions）

与用户定义的标量函数类似，用户定义的表函数，可以将0、1或多个标量值作为输入参数；与标量函数不同的是，它可以返回任意数量的行作为输出，而不是单个值。

为了定义一个表函数，必须扩展org.apache.flink.table.functions中的基类TableFunction并实现（一个或多个）求值方法。表函数的行为由其求值方法决定，求值方法必须是public的，并命名为eval。求值方法的参数类型，决定表函数的所有有效参数。

返回表的类型由TableFunction的泛型类型确定。求值方法使用protected collect(T)方法发出输出行。

在Table API中，Table函数需要与.joinLateral或.leftOuterJoinLateral一起使用。

joinLateral算子，会将外部表中的每一行，与表函数（TableFunction，算子的参数是它的表达式）计算得到的所有行连接起来。

而leftOuterJoinLateral算子，则是左外连接，它同样会将外部表中的每一行与表函数计算生成的所有行连接起来；并且，对于表函数返回的是空表的外部行，也要保留下来。

在SQL中，则需要使用Lateral Table（<TableFunction>），或者带有ON TRUE条件的左连接。

下面的代码中，我们将定义一个表函数，在表环境中注册它，并在查询中调用它。

自定义TableFunction：

```scala
// 自定义TableFunction
  @FunctionHint(output = new DataTypeHint("ROW<word STRING, length INT>"))
  class SplitFunction extends TableFunction[Row] {

    def eval(str: String): Unit = {
      // use collect(...) to emit a row
      str.split("#").foreach(s => collect(Row.of(s, Int.box(s.length))))
    }
  }
```

完整代码：

```scala
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.annotation.{DataTypeHint, FunctionHint}
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

object TableFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .fromElements(
        "hello#world",
        "atguigu#bigdata"
      )

    val settings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .build()

    val tEnv = StreamTableEnvironment.create(env, settings)

    tEnv.createTemporaryView("MyTable", stream, $"s")

    // 注册函数
    tEnv.createTemporarySystemFunction("SplitFunction", classOf[SplitFunction])

    // 在 Table API 里调用注册好的函数
    tEnv
      .from("MyTable")
      .joinLateral(call("SplitFunction", $"s"))
      .select($"s", $"word", $"length")
      .toAppendStream[Row]
      .print()

    tEnv
      .from("MyTable")
      .leftOuterJoinLateral(call("SplitFunction", $"s"))
      .select($"s", $"word", $"length")

    // 在 SQL 里调用注册好的函数
    tEnv.sqlQuery(
      "SELECT s, word, length " +
        "FROM MyTable, LATERAL TABLE(SplitFunction(s))")

    tEnv.sqlQuery(
      "SELECT s, word, length " +
        "FROM MyTable " +
        "LEFT JOIN LATERAL TABLE(SplitFunction(s)) ON TRUE")

    env.execute()
  }

  @FunctionHint(output = new DataTypeHint("ROW<word STRING, length INT>"))
  class SplitFunction extends TableFunction[Row] {

    def eval(str: String): Unit = {
      // use collect(...) to emit a row
      str.split("#").foreach(s => collect(Row.of(s, Int.box(s.length))))
    }
  }
}
```

**java version**

```java
public class TableFunctionExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        DataStreamSource<String> stream = env.fromElements("hello#world", "bigdata#atguigu");

        tEnv.createTemporaryView("t", stream, $("s"));

        // table api
        tEnv
                .from("t")
                .joinLateral(call(SplitFunction.class, $("s")))
                .select($("s"), $("word"), $("length"));
        tEnv
                .from("t")
                .leftOuterJoinLateral(call(SplitFunction.class, $("s")))
                .select($("s"), $("word"), $("length"));

        // rename fields of the function in Table API
        tEnv
                .from("t")
                .leftOuterJoinLateral(call(SplitFunction.class, $("s")).as("newWord", "newLength"))
                .select($("s"), $("newWord"), $("newLength"));

        // sql写法
        // 注册udf函数
        tEnv.createTemporarySystemFunction("SplitFunction", SplitFunction.class);

        Table sqlResult = tEnv
                .sqlQuery("SELECT s, word, length FROM t, LATERAL TABLE(SplitFunction(s))");

        // 和上面的写法等价
        tEnv.sqlQuery(
                "SELECT s, word, length " +
                        "FROM t " +
                        "LEFT JOIN LATERAL TABLE(SplitFunction(s)) ON TRUE");

        tEnv.toAppendStream(sqlResult, Row.class).print();

        env.execute();
    }

    // 类型注解，flink特有的语法
    @FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>"))
    public static class SplitFunction extends TableFunction<Row> {

        public void eval(String str) {
            for (String s : str.split("#")) {
                // use collect(...) to emit a row
                collect(Row.of(s, s.length()));
            }
        }
    }
}
```

#### 聚合函数（Aggregate Functions）

用户自定义聚合函数（User-Defined Aggregate Functions，UDAGGs）可以把一个表中的数据，聚合成一个标量值。用户定义的聚合函数，是通过继承AggregateFunction抽象类实现的。

![](images/udagg-mechanism.png)

上图中显示了一个聚合的例子。

假设现在有一张表，包含了各种饮料的数据。该表由三列（id、name和price）、五行组成数据。现在我们需要找到表中所有饮料的最高价格，即执行max（）聚合，结果将是一个数值。

AggregateFunction的工作原理如下。

* 首先，它需要一个累加器，用来保存聚合中间结果的数据结构（状态）。可以通过调用AggregateFunction的createAccumulator()方法创建空累加器。
* 随后，对每个输入行调用函数的accumulate()方法来更新累加器。
* 处理完所有行后，将调用函数的getValue()方法来计算并返回最终结果。

AggregationFunction要求必须实现的方法：

* createAccumulator()
* accumulate()
* getValue()

除了上述方法之外，还有一些可选择实现的方法。其中一些方法，可以让系统执行查询更有效率，而另一些方法，对于某些场景是必需的。例如，如果聚合函数应用在会话窗口（session group window）的上下文中，则merge()方法是必需的。

* retract()
* merge()
* resetAccumulator()

接下来我们写一个自定义AggregateFunction，计算一下每个sensor的平均温度值。

```scala
// 定义AggregateFunction的Accumulator
class AvgTempAcc {
  var sum: Double = 0.0
  var count: Int = 0
}

class AvgTemp extends AggregateFunction[Double, AvgTempAcc] {
  override def getValue(accumulator: AvgTempAcc): Double = accumulator.sum / accumulator.count

  override def createAccumulator(): AvgTempAcc = new AvgTempAcc

  def accumulate(accumulator: AvgTempAcc, temp: Double): Unit ={
    accumulator.sum += temp
    accumulator.count += 1
  }
}
```

接下来就可以在代码中调用了。

```scala
// 创建一个聚合函数实例
val avgTemp = new AvgTemp()
// Table API的调用
val resultTable = sensorTable
  .groupBy($"id")
  .aggregate(avgTemp($"temperature") as $"avgTemp")
  .select($"id", $"avgTemp")

// SQL的实现
tableEnv.createTemporaryView("sensor", sensorTable)
tableEnv.registerFunction("avgTemp", avgTemp)
val resultSqlTable = tableEnv.sqlQuery(
  """
    |SELECT
    |id, avgTemp(temperature)
    |FROM
    |sensor
    |GROUP BY id
  """.stripMargin)

// 转换成流打印输出
resultTable.toRetractStream[(String, Double)].print("agg temp")
resultSqlTable.toRetractStream[Row].print("agg temp sql")
```

**java version**

```java
public class AggregateFunctionExample {
    public static void main(String[] args) throws Exception {
        // 创建流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置为使用流模式
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        // 创建表环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());

        // 流 -> 表
        Table table = tEnv.fromDataStream(
                stream,
                $("id"),
                $("timestamp").as("ts"),
                $("temperature"),
                $("pt").proctime());

        tEnv.createTemporaryView("sensor", stream);

        tEnv.registerFunction("avgTemp", new AvgTemp());
        Table sqlResult = tEnv.sqlQuery("SELECT id, avgTemp(temperature) FROM sensor GROUP BY id");
        tEnv.toRetractStream(sqlResult, Row.class).print();

        env.execute();
    }

    public static class AvgTempAcc {
        public Double sum = 0.0;
        public Integer count = 0;

        public AvgTempAcc() {
        }

        public AvgTempAcc(Double sum, Integer count) {
            this.sum = sum;
            this.count = count;
        }
    }

    public static class AvgTemp extends AggregateFunction<Double, AvgTempAcc> {
        @Override
        public AvgTempAcc createAccumulator() {
            return new AvgTempAcc();
        }

        public void accumulate(AvgTempAcc acc, Double temp) {
            acc.sum += temp;
            acc.count += 1;
        }

        @Override
        public Double getValue(AvgTempAcc acc) {
            return acc.sum / acc.count;
        }
    }
}
```

#### 表聚合函数（Table Aggregate Functions）

用户定义的表聚合函数（User-Defined Table Aggregate Functions，UDTAGGs），可以把一个表中数据，聚合为具有多行和多列的结果表。这跟AggregateFunction非常类似，只是之前聚合结果是一个标量值，现在变成了一张表。

![](images/udtagg-mechanism.png)

比如现在我们需要找到表中所有饮料的前2个最高价格，即执行top2()表聚合。我们需要检查5行中的每一行，得到的结果将是一个具有排序后前2个值的表。

用户定义的表聚合函数，是通过继承TableAggregateFunction抽象类来实现的。

TableAggregateFunction的工作原理如下。

* 首先，它同样需要一个累加器（Accumulator），它是保存聚合中间结果的数据结构。通过调用TableAggregateFunction的createAccumulator()方法可以创建空累加器。
* 随后，对每个输入行调用函数的accumulate()方法来更新累加器。
* 处理完所有行后，将调用函数的emitValue()方法来计算并返回最终结果。

AggregationFunction要求必须实现的方法：

* createAccumulator()
* accumulate()

除了上述方法之外，还有一些可选择实现的方法。

* retract()
* merge()
* resetAccumulator()
* emitValue()
* emitUpdateWithRetract()

接下来我们写一个自定义TableAggregateFunction，用来提取每个sensor最高的两个温度值。

```scala
// 先定义一个 Accumulator
class Top2TempAcc{
  var highestTemp: Double = Int.MinValue
  var secondHighestTemp: Double = Int.MinValue
}

// 自定义 TableAggregateFunction
class Top2Temp extends TableAggregateFunction[(Double, Int), Top2TempAcc]{

  override def createAccumulator(): Top2TempAcc = new Top2TempAcc

  def accumulate(acc: Top2TempAcc, temp: Double): Unit ={
    if( temp > acc.highestTemp ){
      acc.secondHighestTemp = acc.highestTemp
      acc.highestTemp = temp
    } else if( temp > acc.secondHighestTemp ){
      acc.secondHighestTemp = temp
    }
  }

  def emitValue(acc: Top2TempAcc, out: Collector[(Double, Int)]): Unit ={
    out.collect(acc.highestTemp, 1)
    out.collect(acc.secondHighestTemp, 2)
  }
}
```

接下来就可以在代码中调用了。

```scala
// 创建一个表聚合函数实例
val top2Temp = new Top2Temp()
// Table API的调用
val resultTable = sensorTable
  .groupBy($"id")
  .flatAggregate(top2Temp($"temperature") as ($"temp", $"rank"))
  .select($"id", $"temp", $"rank")

// 转换成流打印输出
resultTable.toRetractStream[(String, Double, Int)].print("agg temp")
resultSqlTable.toRetractStream[Row].print("agg temp sql")
```

**java version**

```java
public class TableAggregateFunctionExample {
    public static void main(String[] args) throws Exception {
        // 创建流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置为使用流模式
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        // 创建表环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        DataStreamSource<SensorReading> stream = env.addSource(new SensorSource());

        tEnv.registerFunction("top2", new Top2Temp());

        // 流 -> 表
        Table table = tEnv.fromDataStream(
                stream,
                $("id"),
                $("timestamp").as("ts"),
                $("temperature"),
                $("pt").proctime());

        Table tableResult = table
                .groupBy($("id"))
                .flatAggregate("top2(temperature) as (temp, rank)")
                .select($("id"), $("temp"), $("rank"));

        tEnv.toRetractStream(tableResult, Row.class).print();

        env.execute();
    }
    public static class Top2TempAcc {
        public Double highestTemp = Double.MIN_VALUE;
        public Double secondHighestTemp = Double.MIN_VALUE;

        public Top2TempAcc(Double highestTemp, Double secondHighestTemp) {
            this.highestTemp = highestTemp;
            this.secondHighestTemp = secondHighestTemp;
        }

        public Top2TempAcc() {
        }
    }

    public static class Top2Temp extends TableAggregateFunction<Tuple2<Double, Integer>, Top2TempAcc> {
        @Override
        public Top2TempAcc createAccumulator() {
            return new Top2TempAcc();
        }

        public void accumulate(Top2TempAcc acc, Double temp) {
            if (temp > acc.highestTemp) {
                acc.secondHighestTemp = acc.highestTemp;
                acc.highestTemp = temp;
            } else if (temp > acc.secondHighestTemp) {
                acc.secondHighestTemp = temp;
            }
        }

        public void emitValue(Top2TempAcc acc, Collector<Tuple2<Double, Integer>> out) {
            out.collect(Tuple2.of(acc.highestTemp, 1));
            out.collect(Tuple2.of(acc.secondHighestTemp, 2));
        }
    }
}
```
