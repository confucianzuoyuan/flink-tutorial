package com.atguigu.day2;

// POJO class
public class SensorReading {
    public String id;
    public long timestamp;
    public double temperature;

    public SensorReading() {}

    public SensorReading(String id, long timestamp, double temperature) {
        this.id = id;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }

    public String toString() {
        return "(" + this.id + ", " + this.timestamp + ", " + this.temperature + ")";
    }
}
