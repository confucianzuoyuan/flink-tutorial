package com.atguigu.day09;

public class PayEvent {
    public String orderId;
    public String eventType;
    public Long timestamp;

    public PayEvent() {
    }

    public PayEvent(String orderId, String eventType, Long timestamp) {
        this.orderId = orderId;
        this.eventType = eventType;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "PayEvent{" +
                "orderId='" + orderId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
