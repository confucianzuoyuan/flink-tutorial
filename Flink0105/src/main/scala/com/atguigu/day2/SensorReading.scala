package com.atguigu.day2

// `id`: 传感器id；`timestamp`：时间戳；`temperature`：温度值
case class SensorReading(id: String,
                         timestamp: Long,
                         temperature: Double)