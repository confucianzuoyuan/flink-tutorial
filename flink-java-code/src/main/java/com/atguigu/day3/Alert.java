package com.atguigu.day3;

//POJO class
public class Alert {
    public String message;
    public long timestamp;

    public Alert() {}

    public Alert(String message, long timestamp) {
        this.message = message;
        this.timestamp = timestamp;
    }

    public String toString() {
        return "(" + message + ", " + timestamp + ")";
    }
}
