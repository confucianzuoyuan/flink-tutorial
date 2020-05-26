### 使用字段表达式来进行keyBy

对于样例类：

```scala
case class SensorReading(
  id: String,
  timestamp: Long,
  temperature: Double
)

val sensorStream: DataStream[SensorReading] = ...
val keyedSensors = sensorStream.keyBy("id")
```

对于元组：

```scala
val input: DataStream[(Int, String, Long)] = ...
val keyed1 = input.keyBy("2") // key by 3rd field
val keyed2 = input.keyBy("_1") // key by 1st field

DataStream<Tuple3<Integer, String, Long>> javaInput = ...
javaInput.keyBy("f2") // key Java tuple by 3rd field
```

对于存在嵌套的样例类：

```scala
case class Address (
  address: String,
  zip: String,
  country: String
)

case class Person (
  name: String,
  birthday: (Int, Int, Int), // year, month, day
  address: Address
)

val persons: DataStream[Person] = ...
persons.keyBy("address.zip") // key by nested POJO field
persons.keyBy("birthday._1") // key by field of nested tuple
persons.keyBy("birthday._") // key by all fields of nested tuple
```

