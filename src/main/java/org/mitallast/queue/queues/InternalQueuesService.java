package org.mitallast.queue.queues;

import com.google.inject.Inject;
import org.mitallast.queue.QueueException;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.Queue;
import org.mitallast.queue.queue.QueueType;
import org.mitallast.queue.queue.service.QueueService;
import org.mitallast.queue.queue.service.StringQueueService;
import org.mitallast.queue.queues.stats.QueueStats;
import org.mitallast.queue.queues.stats.QueuesStats;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class InternalQueuesService extends AbstractLifecycleComponent implements QueuesService {

    private volatile Map<String, QueueService> queues = new HashMap<>();

    @Inject
    public InternalQueuesService(Settings settings) {
        super(settings);
    }

    @Override
    protected void doStart() throws QueueException {
        for (QueueService queueService : queues.values()) {
            queueService.start();
        }
    }

    @Override
    protected void doStop() throws QueueException {
        for (QueueService queueService : queues.values()) {
            queueService.stop();
        }
    }

    @Override
    protected void doClose() throws QueueException {
        for (QueueService queueService : queues.values()) {
            queueService.close();
        }
    }

    @Override
    public boolean hasQueue(String name) {
        return queues.containsKey(name);
    }

    @Override
    public Set<String> queues() {
        return queues.keySet();
    }

    @Override
    public QueueService queue(String name) {
        return queues.get(name);
    }

    @Override
    public synchronized QueueService createQueue(String name, QueueType type, Settings queueSettings) {
        Queue queue = new Queue(name);
        if (queues.containsKey(queue.getName())) {
            throw new QueueAlreadyExistsException(queue.getName());
        }

        QueueService queueService;
        switch (type) {
            case STRING:
                queueService = new StringQueueService(settings, queueSettings, queue);
                break;
            default:
                throw new QueueMissingException("Queue type missing");
        }

        queueService.start();
        queues.put(queueService.queue().getName(), queueService);
        return queueService;
    }

    @Override
    public synchronized void deleteQueue(String name, String reason) {
        logger.info("delete queue {} reason {}", name, reason);
        Queue queue = new Queue(name);
        QueueService queueService = queues.get(queue.getName());
        if (queueService == null) {
            throw new QueueMissingException("Queue not found");
        }
        queueService.removeQueue();
        queues.remove(queue.getName());
        logger.info("queue deleted");
    }

    @Override
    public QueuesStats stats() {
        QueuesStats stats = new QueuesStats();
        for (QueueService queueService : queues.values()) {
            stats.addQueueStats(queueService.stats());
        }
        return stats;
    }

    @Override
    public QueueStats stats(String name) {
        Queue queue = new Queue(name);
        QueueService queueService = queues.get(queue.getName());
        if (queueService == null) {
            throw new QueueMissingException("Queue not found");
        }
        return queueService.stats();
    }
}
