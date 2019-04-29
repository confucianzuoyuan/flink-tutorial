package com.atguigu.day02.util;

public class Alert {
    public String message;
    public long timestamp;

    public Alert() {
    }

    public Alert(String message, long timestamp) {
        this.message = message;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Alert{" +
                "message='" + message + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
