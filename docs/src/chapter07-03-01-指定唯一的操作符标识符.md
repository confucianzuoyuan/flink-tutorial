### 指定唯一的操作符标识符

每一个操作符都可以指定唯一的标识符。标识符将会作为操作符的元数据和状态数据一起保存到savepoint中去。当应用从保存点恢复时，标识符可以用来在savepoint中查找标识符对应的操作符的状态数据。标识符必须是唯一的，否则应用不知道从哪一个标识符恢复。

强烈建议为应用的每一个操作符定义唯一标识符。例子：

```java
DataStream<Tuple3<String, Double, Double>> alerts = keyedSensorData
  .flatMap(new TemperatureAlertFunction(1.1))  
  .uid("TempAlert");
```

