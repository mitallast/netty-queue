package org.mitallast.queue.action;

public enum ActionType {
    QUEUE_DELETE(0x1001),
    QUEUE_DEQUEUE(0x1002),
    QUEUE_ENQUEUE(0x1003),
    QUEUE_GET(0x1004),
    QUEUE_PEEK(0x1005),
    QUEUE_STATS(0x1006),

    QUEUES_CREATE(0x2001),
    QUEUES_DELETE(0x2002),
    QUEUES_STATS(0x2003);

    private final int id;

    ActionType(int id) {
        this.id = id;
    }

    public int id() {
        return id;
    }
}
