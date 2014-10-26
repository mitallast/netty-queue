package org.mitallast.queue.queue.service;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mitallast.queue.common.settings.ImmutableSettings;
import org.mitallast.queue.queue.Queue;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queue.QueueMessageUuidDuplicateException;

import java.io.IOException;
import java.util.UUID;

public class LevelDbQueueServiceTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private LevelDbQueueService levelDbQueueService;

    @Before
    public void before() {
        levelDbQueueService = new LevelDbQueueService(
            ImmutableSettings.builder()
                .put("work_dir", folder.getRoot().getPath())
                .build(),
            ImmutableSettings.builder().build(),
            new Queue("test_queue")
        );
        levelDbQueueService.start();
    }

    @After
    public void after() {
        levelDbQueueService.close();
        levelDbQueueService = null;
    }

    @Test
    public void testEmpty() {
        assert null == levelDbQueueService.peek();
    }

    @Test
    public void testEnqueueAndPeek() throws IOException {
        QueueMessage<String> message = message();
        levelDbQueueService.enqueue(message);
        QueueMessage<String> peek = levelDbQueueService.peek();
        assert peek != null;
        assert message.equals(peek);
    }

    @Test
    public void testEnqueueAndDequeue() throws IOException {
        QueueMessage<String> message = message();
        levelDbQueueService.enqueue(message);
        QueueMessage<String> peek = levelDbQueueService.dequeue();
        assert peek != null;
        assert message.equals(peek);
    }

    @Test(expected = QueueMessageUuidDuplicateException.class)
    public void testEnqueueDuplicate() throws IOException {
        QueueMessage<String> message = message();
        levelDbQueueService.enqueue(message);
        levelDbQueueService.enqueue(message);
    }

    @Test
    public void testUUID() throws IOException {
        for (int i = 0; i < 1000; i++) {
            UUID expected = UUID.randomUUID();
            UUID actual = LevelDbQueueService.toUUID(LevelDbQueueService.toBytes(expected));
            assert expected.equals(actual);
        }
    }

    @Test
    public void testGet() throws IOException {
        QueueMessage<String> message = message();
        levelDbQueueService.enqueue(message);
        QueueMessage<String> saved = levelDbQueueService.get(message.getUuid());
        assert message.equals(saved);
    }

    private QueueMessage<String> message() {
        QueueMessage<String> message = new QueueMessage<>();
        message.setMessage("Hello world");
        return message;
    }
}
