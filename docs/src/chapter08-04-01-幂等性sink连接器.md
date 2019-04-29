### 幂等sink连接器

对于大多数应用，SinkFunction接口足以实现一个幂等性写入的sink连接器了。需要以下两个条件：

* 结果数据必须具有确定性的key，在这个key上面幂等性更新才能实现。例如一个计算每分钟每个传感器的平均温度值的程序，确定性的key值可以是传感器的ID和每分钟的时间戳。确定性的key值，对于在故障恢复的场景下，能够正确的覆盖结果非常的重要。
* 外部系统支持针对每个key的更新，例如关系型数据库或者key-value存储。

下面的例子展示了如何实现一个针对JDBC数据库的幂等写入sink连接器，这里使用的是Apache Derby数据库。

```java
val readings: DataStream[SensorReading] = ...

// write the sensor readings to a Derby table
readings.addSink(new DerbyUpsertSink)

// -----

class DerbyUpsertSink extends RichSinkFunction[SensorReading] {
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    // connect to embedded in-memory Derby
    conn = DriverManager.getConnection(
       "jdbc:derby:memory:flinkExample",
       new Properties())
    // prepare insert and update statements
    insertStmt = conn.prepareStatement(
      "INSERT INTO Temperatures (sensor, temp) VALUES (?, ?)")
    updateStmt = conn.prepareStatement(
      "UPDATE Temperatures SET temp = ? WHERE sensor = ?")
  }

  override def invoke(SensorReading r, context: Context[_]): Unit = {
    // set parameters for update statement and execute it
    updateStmt.setDouble(1, r.temperature)
    updateStmt.setString(2, r.id)
    updateStmt.execute()
    // execute insert statement
    // if update statement did not update any row
    if (updateStmt.getUpdateCount == 0) {
      // set parameters for insert statement
      insertStmt.setString(1, r.id)
      insertStmt.setDouble(2, r.temperature)
      // execute insert statement
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

由于Apache Derby并没有提供内置的UPSERT方法，所以这个sink连接器实现了UPSERT写。具体实现方法是首先去尝试更新一行数据，如果这行数据不存在，则插入新的一行数据。

