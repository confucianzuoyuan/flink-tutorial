### JDBC自定义sink

```xml
<dependency>
  <groupId>mysql</groupId>
  <artifactId>mysql-connector-java</artifactId>
  <version>5.1.44</version>
</dependency>
```

添加MyJdbcSink

```scala
class MyJdbcSink() extends RichSinkFunction[SensorReading]{
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _
  // open 主要是创建连接
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    conn = DriverManager.getConnection(
      "jdbc:mysql://localhost:3306/test",
      "root",
      "123456")
    insertStmt = conn.prepareStatement(
      "INSERT INTO temperatures (sensor, temp) VALUES (?, ?)"
    )
    updateStmt = conn.prepareStatement(
      "UPDATE temperatures SET temp = ? WHERE sensor = ?"
    )
  }
  // 调用连接，执行sql
  override def invoke(value: SensorReading,
                      context: SinkFunction.Context[_]): Unit = {
    updateStmt.setDouble(1, value.temperature)
    updateStmt.setString(2, value.id)
    updateStmt.execute()
    
    if (updateStmt.getUpdateCount == 0) {
      insertStmt.setString(1, value.id)
      insertStmt.setDouble(2, value.temperature)
      insertStmt.execute()
    }
  }
  
  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}
```

在main方法中增加，把明细保存到mysql中

```scala
dataStream.addSink(new MyJdbcSink())
```

