package org.mitallast.queue.queues.stats;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class QueuesStats implements Streamable {
    private List<QueueStats> queueStats = new ArrayList<>();

    public void addQueueStats(QueueStats queueStats) {
        this.queueStats.add(queueStats);
    }

    public List<QueueStats> getQueueStats() {
        return queueStats;
    }

    @Override
    public void readFrom(StreamInput stream) throws IOException {
        int size = stream.readInt();
        queueStats = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            QueueStats queueStat = new QueueStats();
            queueStat.readFrom(stream);
            queueStats.add(queueStat);
        }
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        if (queueStats == null || queueStats.isEmpty()) {
            stream.writeInt(0);
        } else {
            stream.writeInt(queueStats.size());
            for (QueueStats queueStat : queueStats) {
                queueStat.writeTo(stream);
            }
        }
    }
}
