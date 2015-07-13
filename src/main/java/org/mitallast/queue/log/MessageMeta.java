package org.mitallast.queue.log;

public class MessageMeta {
    // message offset
    protected final long offset;
    // message position
    protected final long position;
    // message length
    protected final int length;
    // message status
    protected final MessageStatus status;

    public MessageMeta(long offset, long position, int length, int status) {
        this(offset, position, length, MessageStatus.values()[status]);
    }

    public MessageMeta(long offset, long position, int length, MessageStatus status) {
        this.offset = offset;
        this.position = position;
        this.length = length;
        this.status = status;
    }
}
