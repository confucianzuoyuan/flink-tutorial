package com.atguigu.day10;

public class ApacheLogEvent {
    //   case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)
    public String ip;
    public String uesrId;
    public Long eventTime;
    public String method;
    public String url;

    public ApacheLogEvent() {
    }

    public ApacheLogEvent(String ip, String uesrId, Long eventTime, String method, String url) {
        this.ip = ip;
        this.uesrId = uesrId;
        this.eventTime = eventTime;
        this.method = method;
        this.url = url;
    }

    @Override
    public String toString() {
        return "ApacheLogEvent{" +
                "ip='" + ip + '\'' +
                ", uesrId='" + uesrId + '\'' +
                ", eventTime=" + eventTime +
                ", method='" + method + '\'' +
                ", url='" + url + '\'' +
                '}';
    }
}
