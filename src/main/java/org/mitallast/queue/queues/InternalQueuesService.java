package org.mitallast.queue.queues;

import org.mitallast.queue.common.module.AbstractComponent;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.Queue;
import org.mitallast.queue.queue.QueueType;
import org.mitallast.queue.queue.service.QueueService;
import org.mitallast.queue.queue.service.StringQueueService;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class InternalQueuesService extends AbstractComponent implements QueuesService {

    private volatile Map<String, QueueService> queues = new HashMap<String, QueueService>();

    public InternalQueuesService(Settings settings) {
        super(settings);
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

        queues.put(queueService.queue().getName(), queueService);
        return queueService;
    }

    @Override
    public synchronized void removeQueue(String name, String reason) {
        Queue queue = new Queue(name);
        QueueService queueService = queues.get(queue.getName());
        if (queueService == null) {
            throw new QueueMissingException("Queue not found");
        }
        queueService.deleteQueue();
        queues.remove(queue.getName());
    }
}
