package com.atguigu.day2

// （温度传感器ID， 时间戳，温度值）
case class SensorReading(id: String,
                         timestamp: Long,
                         temperature: Double)