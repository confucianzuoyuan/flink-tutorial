package atguigu.entity;

public class LoginEvent {
    private String userId;
    private String ip;
    private String type;
    private String eventTime;

    public LoginEvent() {
    }

    public LoginEvent(String userId, String ip, String type, String eventTime) {
        this.userId = userId;
        this.ip = ip;
        this.type = type;
        this.eventTime = eventTime;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public void setEventTime(String eventTime) {
        this.eventTime = eventTime;
    }

    public String getEventTime() {
        return eventTime;
    }
}