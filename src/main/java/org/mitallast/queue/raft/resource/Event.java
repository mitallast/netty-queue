package org.mitallast.queue.raft.resource;

public class Event {

    private final Type type;
    private final String path;

    public Event(Type type, String path) {
        this.type = type;
        this.path = path;
    }

    public Type type() {
        return type;
    }

    public String path() {
        return path;
    }

    public enum Type {
        CREATE_PATH,
        CREATE_RESOURCE,
        STATE_CHANGE,
        DELETE_PATH,
        DELETE_RESOURCE
    }
}
