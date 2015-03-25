package org.mitallast.queue.queue.service;

import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.common.settings.ImmutableSettings;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.Queue;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queue.QueueMessageUuidDuplicateException;

import java.io.IOException;
import java.util.List;

public abstract class BaseQueueServiceTest<T extends QueueService> extends BaseTest {

    protected T queueService;

    protected abstract T createQueueService(Settings settings, Settings queueSettings, Queue queue);

    @Before
    public void setUp() throws Exception {
        queueService = createQueueService(
            ImmutableSettings.builder()
                .put("work_dir", testFolder.getRoot().getPath())
                .build(),
            ImmutableSettings.builder().build(),
            new Queue("test_queue")
        );
        queueService.start();
    }

    @After
    public void tearDown() throws Exception {
        queueService.close();
        queueService = null;
    }

    @Test
    public void testEmpty() {
        assert null == queueService.peek();
    }

    @Test
    public void testEnqueueAndPeek() throws IOException {
        QueueMessage message = createMessage();
        queueService.enqueue(message);
        QueueMessage peek = queueService.peek();
        assert peek != null;
        assert message.equals(peek);
    }

    @Test
    public void testEnqueueAndDequeue() throws IOException {
        QueueMessage message1 = createMessage();
        QueueMessage message2 = createMessage();
        QueueMessage message3 = createMessage();
        queueService.enqueue(message1);
        queueService.enqueue(message2);
        queueService.enqueue(message3);
        QueueMessage peek1 = queueService.dequeue();
        QueueMessage peek2 = queueService.dequeue();
        QueueMessage peek3 = queueService.dequeue();
        assert peek1 != null;
        assert peek2 != null;
        assert peek3 != null;
        assert message1.equals(peek1) : message1 + " != " + peek1;
        assert message2.equals(peek2) : message2 + " != " + peek2;
        assert message3.equals(peek3) : message3 + " != " + peek3;
    }

    @Test(expected = QueueMessageUuidDuplicateException.class)
    public void testEnqueueDuplicate() throws IOException {
        QueueMessage message = createMessage();
        queueService.enqueue(message);
        queueService.enqueue(message);
    }

    @Test
    public void testGet() throws IOException {
        QueueMessage message = createMessage();
        queueService.enqueue(message);
        QueueMessage saved = queueService.get(message.getUuid());
        assert message.equals(saved);
    }

    @Test
    public void testEnqueueAndGetConcurrent() throws Exception {
        executeConcurrent((Task) () -> {
            for (int i = 0; i < max(); i++) {
                QueueMessage queueMessage = createMessage();
                queueService.enqueue(queueMessage);
                assert queueMessage.getUuid() != null;
                QueueMessage queueMessageActual = queueService.get(queueMessage.getUuid());
                assert queueMessage.equals(queueMessageActual)
                    : queueMessage + " != " + queueMessageActual;
            }
        });
        assert queueService.size() == total();
    }

    @Test
    public void testEnqueueAndDeQueueConcurrent() throws Exception {
        executeConcurrent((Task) () -> {
            for (int i = 0; i < max(); i++) {
                QueueMessage queueMessage = createMessage();
                queueService.enqueue(queueMessage);
            }
        });
        assert queueService.size() == total();
        executeConcurrent((Task) () -> {
            for (int i = 0; i < max(); i++) {
                QueueMessage queueMessage = queueService.dequeue();
                assert queueMessage != null;
                assert queueMessage.getUuid() != null;
            }
        });
    }

    @Test
    public void testEnqueuePerformance() throws Exception {
        warmUp();
        List<QueueMessage> messages = createMessages();
        long start = System.currentTimeMillis();
        messages.forEach(queueService::enqueue);
        long end = System.currentTimeMillis();
        printQps("enqueue", max(), start, end);
    }

    @Test
    public void testEnqueuePerformanceConcurrent() throws Exception {
        warmUp();
        List<QueueMessage> messages = createMessages();
        long start = System.currentTimeMillis();
        executeConcurrent((thread, concurrency) -> {
            for (int i = thread; i < max(); i += concurrency) {
                queueService.enqueue(messages.get(i));
            }
        });
        long end = System.currentTimeMillis();
        printQps("enqueue concurrent", max(), start, end);
    }

    @Test
    public void testEnqueuePerformanceWithUid() throws Exception {
        warmUp();
        ImmutableList<QueueMessage> messages = createMessagesWithUuid();
        long start = System.currentTimeMillis();
        messages.forEach(queueService::enqueue);
        long end = System.currentTimeMillis();
        printQps("enqueue with uuid", max(), start, end);
    }

    @Test
    public void testEnqueuePerformanceWithUidConcurrent() throws Exception {
        warmUp();
        ImmutableList<QueueMessage> messages = createMessagesWithUuid();
        long start = System.currentTimeMillis();
        executeConcurrent((thread, concurrency) -> {
            for (int i = thread; i < max(); i += concurrency) {
                queueService.enqueue(messages.get(i));
            }
        });
        long end = System.currentTimeMillis();
        printQps("enqueue with uuid concurrent", max(), start, end);
    }

    @Test
    public void testDequeuePerformance() throws Exception {
        warmUp();
        createMessages().forEach(queueService::enqueue);
        long start = System.currentTimeMillis();
        for (int i = 0; i < max(); i++) {
            QueueMessage message = queueService.dequeue();
            assert message != null;
            assert message.getUuid() != null;
        }
        long end = System.currentTimeMillis();
        printQps("dequeue", max(), start, end);
    }

    @Test
    public void testDequeuePerformanceWithUuid() throws Exception {
        warmUp();
        createMessagesWithUuid().forEach(queueService::enqueue);
        long start = System.currentTimeMillis();
        for (int i = 0; i < max(); i++) {
            QueueMessage message = queueService.dequeue();
            assert message != null;
            assert message.getUuid() != null;
        }
        long end = System.currentTimeMillis();
        printQps("dequeue with uuid", max(), start, end);
    }

    @Test
    public void testDequeuePerformanceConcurrent() throws Exception {
        warmUp();
        createMessages(total()).forEach(queueService::enqueue);
        long start = System.currentTimeMillis();
        executeConcurrent((Task) () -> {
            for (int i = 0; i < max(); i++) {
                QueueMessage message = queueService.dequeue();
                assert message != null;
                assert message.getUuid() != null;
            }
        });
        long end = System.currentTimeMillis();
        printQps("dequeue concurrent", total(), start, end);
    }

    @Test
    public void testDequeuePerformanceConcurrentWithUuid() throws Exception {
        warmUp();
        createMessagesWithUuid(total()).forEach(queueService::enqueue);
        long start = System.currentTimeMillis();
        executeConcurrent((Task) () -> {
            for (int i = 0; i < max(); i++) {
                QueueMessage message = queueService.dequeue();
                assert message != null;
                assert message.getUuid() != null;
            }
        });
        long end = System.currentTimeMillis();
        printQps("dequeue concurrent with uuid", total(), start, end);
    }

    private void warmUp() throws Exception {
        for (int i = 0; i < max(); i++) {
            queueService.enqueue(createMessageWithUuid());
        }
        for (int i = 0; i < max(); i++) {
            QueueMessage message = queueService.dequeue();
            assert message != null;
            assert message.getUuid() != null;
        }
    }
}
