package com.atguigu.day10;

import java.sql.Timestamp;

public class UrlViewCount {
    // case class UrlViewCount(url: String, windowEnd: Long, count: Long)
    public String url;
    public Long windowEnd;
    public Long count;

    public UrlViewCount() {
    }

    public UrlViewCount(String url, Long windowEnd, Long count) {
        this.url = url;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    @Override
    public String toString() {
        return "UrlViewCount{" +
                "url='" + url + '\'' +
                ", windowEnd=" + new Timestamp(windowEnd) +
                ", count=" + count +
                '}';
    }
}
