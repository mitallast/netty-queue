package org.mitallast.queue.queues;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.inject.Inject;
import org.mitallast.queue.QueueException;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;
import org.mitallast.queue.common.settings.ImmutableSettings;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.Queue;
import org.mitallast.queue.queue.service.LevelDbQueueService;
import org.mitallast.queue.queue.service.QueueService;
import org.mitallast.queue.queues.stats.QueueStats;
import org.mitallast.queue.queues.stats.QueuesStats;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class InternalQueuesService extends AbstractLifecycleComponent implements QueuesService {

    private final static String stateFileName = "queues.json";
    private final Map<String, QueueService> queues = new HashMap<>();

    @Inject
    public InternalQueuesService(Settings settings) {
        super(settings);
    }

    @Override
    protected void doStart() throws QueueException {
        if (queues.isEmpty()) {
            loadState();
        } else {
            for (QueueService queueService : queues.values()) {
                queueService.start();
            }
        }
        flushState();
    }

    @Override
    protected void doStop() throws QueueException {
        for (QueueService queueService : queues.values()) {
            queueService.stop();
        }
        flushState();
    }

    @Override
    protected void doClose() throws QueueException {
        flushState();
        for (QueueService queueService : queues.values()) {
            queueService.close();
        }
        queues.clear();
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
    public synchronized QueueService createQueue(String name, Settings queueSettings) {
        logger.info("create queue {}, {}", name, queueSettings);
        Queue queue = new Queue(name);
        if (queues.containsKey(queue.getName())) {
            throw new QueueAlreadyExistsException(queue.getName());
        }

        final QueueService queueService = new LevelDbQueueService(settings, queueSettings, queue);

        queueService.start();
        queues.put(queueService.queue().getName(), queueService);
        flushState();
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
        flushState();
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

    private synchronized void loadState() throws QueueException {
        File stateFile = getStateFile();
        if (stateFile.exists()) {
            try (FileInputStream inputStream = new FileInputStream(stateFile)) {
                JsonFactory factory = new JsonFactory();
                JsonParser parser = factory.createParser(inputStream);
                assertEquals(JsonToken.START_OBJECT, parser.nextToken());
                assertEquals(JsonToken.FIELD_NAME, parser.nextToken());
                assertEquals("queues", parser.getCurrentName());
                assertEquals(JsonToken.START_ARRAY, parser.nextToken());

                JsonToken token;
                while ((token = parser.nextToken()) != JsonToken.END_ARRAY) {
                    assertEquals(JsonToken.START_OBJECT, token);
                    String queue = null;
                    ImmutableSettings.Builder builder = ImmutableSettings.builder();
                    while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {
                        assertEquals(JsonToken.FIELD_NAME, token);
                        switch (parser.getCurrentName()) {
                            case "queue":
                                assertEquals(JsonToken.VALUE_STRING, parser.nextToken());
                                queue = parser.getText();
                                break;
                            case "settings":
                                assertEquals(JsonToken.START_OBJECT, parser.nextToken());
                                String currentFieldName;
                                String currentFieldValue;
                                while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {
                                    assertEquals(JsonToken.FIELD_NAME, token);
                                    currentFieldName = parser.getCurrentName();
                                    assertEquals(JsonToken.VALUE_STRING, parser.nextToken());
                                    currentFieldValue = parser.getText();
                                    builder.put(currentFieldName, currentFieldValue);
                                }
                                break;
                        }
                    }
                    if (queue == null) {
                        throw new QueueException("Queue name cannot be null");
                    }
                    createQueue(queue, builder.build());
                }
                assertEquals(JsonToken.END_OBJECT, parser.nextToken());
                parser.close();
            } catch (IOException e) {
                logger.error("error read queues state", e);
                throw new QueueException(e);
            }
        }
    }

    private synchronized void flushState() throws QueueException {
        logger.info("flush state {}", queues.keySet());
        File outputFile = getStateFile();
        if (!outputFile.exists()) {
            try {
                if (!outputFile.createNewFile()) {
                    throw new IOException("Error create file " + outputFile);
                }
            } catch (IOException e) {
                throw new QueueException(e);
            }
        }
        try (FileOutputStream outputStream = new FileOutputStream(outputFile)) {
            JsonFactory factory = new JsonFactory();
            JsonGenerator generator = factory.createGenerator(outputStream);
            generator.writeStartObject();
            generator.writeFieldName("queues");
            generator.writeStartArray();
            for (QueueService queueService : queues.values()) {
                generator.writeStartObject();
                generator.writeStringField("queue", queueService.queue().getName());
                generator.writeFieldName("settings");
                generator.writeStartObject();
                for (Map.Entry<String, String> settingsEntry : queueService.queueSettings().getAsMap().entrySet()) {
                    generator.writeStringField(settingsEntry.getKey(), settingsEntry.getValue());
                }
                generator.writeEndObject();
                generator.writeEndObject();
            }
            generator.writeEndArray();
            generator.writeEndObject();
            generator.close();
        } catch (IOException e) {
            throw new QueueException(e);
        }
    }

    private File getStateFile() {
        return new File(settings.get("work_dir"), stateFileName);
    }

    private <T> void assertEquals(T expected, T actual) {
        if (expected != actual) {
            throw new AssertionError("Expected " + expected + ", actual " + actual);
        }
    }
}
