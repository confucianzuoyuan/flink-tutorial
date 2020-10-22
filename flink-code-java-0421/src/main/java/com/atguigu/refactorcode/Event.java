package com.atguigu.refactorcode;

public class Event {
    public String key;
    public Long value;
    public Long timestamp;

    public Event(String key, Long value, Long timestamp) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }

    public Event() {
    }

    @Override
    public String toString() {
        return "Event{" + "key='" + key + '\'' + ", value='" + value + '\'' + ", timestamp=" + timestamp + '}';
    }
}
