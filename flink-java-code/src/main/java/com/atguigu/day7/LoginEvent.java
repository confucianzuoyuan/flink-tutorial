package com.atguigu.day7;

public class LoginEvent {
    public String userId;
    public String ipAddress;
    public String eventType;
    public Long eventTime;

    public LoginEvent(String userId, String ipAddress, String eventType, Long eventTime) {
        this.userId = userId;
        this.ipAddress = ipAddress;
        this.eventType = eventType;
        this.eventTime = eventTime;
    }

    public LoginEvent() {}

    @Override
    public String toString() {
        return "LoginEvent{" +
                "userId='" + userId + '\'' +
                ", ipAddress='" + ipAddress + '\'' +
                ", eventType='" + eventType + '\'' +
                ", eventTime=" + eventTime +
                '}';
    }
}
