package org.mitallast.queue.queues.stats;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.queue.Queue;

import java.io.IOException;

public class QueueStats implements Streamable {
    private Queue queue;
    private long size;

    public Queue getQueue() {
        return queue;
    }

    public void setQueue(Queue queue) {
        this.queue = queue;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    @Override
    public void readFrom(StreamInput stream) throws IOException {
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeTextOrNull(queue != null ? queue.getName() : null);
        stream.writeLong(size);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        QueueStats stats = (QueueStats) o;

        if (size != stats.size) {
            return false;
        }
        if (queue != null ? !queue.equals(stats.queue) : stats.queue != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = queue != null ? queue.hashCode() : 0;
        result = 31 * result + (int) (size ^ (size >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "QueueStats{" +
            "queue=" + queue +
            ", size=" + size +
            '}';
    }
}
