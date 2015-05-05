package org.mitallast.queue.action;

public enum ActionType {
    QUEUE_DELETE(0x1001),
    QUEUE_POP(0x1002),
    QUEUE_PUSH(0x1003),
    QUEUE_GET(0x1004),
    QUEUE_PEEK(0x1005),
    QUEUE_STATS(0x1006),

    TRANSACTION_COMMIT(0x3001),
    TRANSACTION_ROLLBACK(0x3002),
    TRANSACTION_PUSH(0x3003),
    TRANSACTION_POP(0x3004),
    TRANSACTION_DELETE(0x3005),

    QUEUES_CREATE(0x2001),
    QUEUES_DELETE(0x2002),
    QUEUES_STATS(0x2003),

    CLUSTER_CONNECT(0x4001),;

    private final int id;

    ActionType(int id) {
        this.id = id;
    }

    public int id() {
        return id;
    }
}
