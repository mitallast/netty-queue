package org.mitallast.queue.queue.service;

import org.mitallast.queue.QueueException;
import org.mitallast.queue.QueueRuntimeException;
import org.mitallast.queue.common.bigqueue.BigQueueImpl;
import org.mitallast.queue.common.bigqueue.IBigQueue;
import org.mitallast.queue.common.bigqueue.utils.FileUtil;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.AbstractQueueComponent;
import org.mitallast.queue.queue.Queue;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queue.QueueType;

import java.io.File;
import java.io.IOException;

public class StringQueueService extends AbstractQueueComponent implements QueueService<String> {

    private final static String BIG_QUEUE_DIR = "big_queue";

    private String workDir;
    private String queueDir;

    private IBigQueue bigQueue;

    public StringQueueService(Settings settings, Settings queueSettings, Queue queue) {
        super(settings, queueSettings, queue);
        workDir = this.settings.get("work_dir");
        if (!workDir.endsWith(File.separator)) {
            workDir += File.separator;
        }
        queueDir = workDir + BIG_QUEUE_DIR;
    }

    @Override
    public void enqueue(QueueMessage<String> o) {
        try {
            bigQueue.enqueue(o.getMessage().getBytes());
        } catch (IOException e) {
            throw new QueueRuntimeException(e);
        }
    }

    @Override
    public QueueMessage<String> dequeue() {
        try {
            byte[] bytes = bigQueue.dequeue();
            if (bytes == null) {
                return null;
            }
            return new QueueMessage<>(new String(bytes));
        } catch (IOException e) {
            throw new QueueRuntimeException(e);
        }
    }

    @Override
    public long size() {
        return bigQueue.size();
    }

    @Override
    public QueueType type() {
        return QueueType.STRING;
    }

    @Override
    public void removeQueue() {
        try {
            logger.info("close queue");
            close();
            logger.info("delete directory");
            FileUtil.deleteDirectory(new File(queueDir));
            logger.info("directory deleted");
        } catch (Throwable e) {
            throw new QueueRuntimeException(e);
        }
    }

    @Override
    public boolean isSupported(QueueMessage message) {
        return message.getMessage() instanceof String;
    }

    @Override
    protected void doStart() throws QueueException {
        try {
            bigQueue = new BigQueueImpl(queueDir, queue.getName(), 1024 * 1024 * 1024);
        } catch (IOException e) {
            throw new QueueRuntimeException(e);
        }
    }

    @Override
    protected void doStop() throws QueueException {
        this.bigQueue.flush();
        try {
            this.bigQueue.close();
        } catch (IOException e) {
            throw new QueueException(e);
        }
    }

    @Override
    protected void doClose() throws QueueException {
    }
}
