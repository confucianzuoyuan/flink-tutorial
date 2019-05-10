package atguigu.entity;

public class Alert {

    private String message;

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Alert(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        return "Alert [message=" + message + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((message == null) ? 0 : message.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if(this == obj) return true;
        if(obj == null) return false;
        if(getClass() != obj.getClass()) return false;

        Alert other = (Alert) obj;

        if(message == null) {
            if(other.message != null) {
                return false;
            }else if(!message.equals(other.message)) {
                return false;
            }
        }
        return true;
    }
}