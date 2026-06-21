package cis5550.webserver;

import java.util.HashMap;
import java.util.Map;

class SessionImpl implements Session {

    private String sessionId;
    private long creationTime;
    private long lastAccessedTime;
    private int maxActiveInterval;
    private Map<String, Object> attributes;
    private boolean valid;

    SessionImpl(String sessionId) {
        this.sessionId = sessionId;
        this.creationTime = System.currentTimeMillis();
        this.lastAccessedTime = this.creationTime;
        this.maxActiveInterval = 300; // default 5 minutes
        this.attributes = new HashMap<>();
        this.valid = true;
    }

    public String id() {
        return sessionId;
    }

    public long creationTime() {
        return creationTime;
    }

    public long lastAccessedTime() {
        return lastAccessedTime;
    }

    public void maxActiveInterval(int seconds) {
        this.maxActiveInterval = seconds;
    }

    public int getMaxActiveInterval() {
        return maxActiveInterval;
    }

    public void invalidate() {
        this.valid = false;
    }

    public boolean isValid() {
        return valid;
    }

    public Object attribute(String name) {
        return attributes.get(name);
    }

    public void attribute(String name, Object value) {
        attributes.put(name, value);
    }

    void setLastAccessedTime(long time) {
        this.lastAccessedTime = time;
    }
}
