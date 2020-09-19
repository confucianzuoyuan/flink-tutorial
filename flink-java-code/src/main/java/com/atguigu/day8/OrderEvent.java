package com.atguigu.day8;

public class OrderEvent {
    public String OrderId;
    public String OrderType;
    public Long OrderTime;

    public OrderEvent(String orderId, String orderType, Long orderTime) {
        OrderId = orderId;
        OrderType = orderType;
        OrderTime = orderTime;
    }

    public OrderEvent() { }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "OrderId='" + OrderId + '\'' +
                ", OrderType='" + OrderType + '\'' +
                ", OrderTime=" + OrderTime +
                '}';
    }
}
