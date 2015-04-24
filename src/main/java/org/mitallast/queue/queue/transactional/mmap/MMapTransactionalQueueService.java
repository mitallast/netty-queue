package org.mitallast.queue.queue.transactional.mmap;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.collect.ImmutableList;
import org.mitallast.queue.QueueException;
import org.mitallast.queue.common.UUIDs;
import org.mitallast.queue.common.mmap.MemoryMappedFile;
import org.mitallast.queue.common.mmap.MemoryMappedFileFactory;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.Queue;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queue.QueueMessageUuidDuplicateException;
import org.mitallast.queue.queue.transactional.AbstractQueueService;
import org.mitallast.queue.queue.transactional.QueueTransaction;
import org.mitallast.queue.queue.transactional.TransactionalQueueService;
import org.mitallast.queue.queue.transactional.mmap.data.MMapQueueMessageAppendSegment;
import org.mitallast.queue.queue.transactional.mmap.meta.MMapQueueMessageMetaSegment;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

public class MMapTransactionalQueueService extends AbstractQueueService implements TransactionalQueueService {

    private final String workDir;
    private final int segmentMaxSize;
    private final float segmentLoadFactor;
    private final ReentrantLock segmentsLock = new ReentrantLock();
    private final ConcurrentMap<UUID, MMapMemoryQueueTransaction> transactionMap;
    private File queueDir;
    private MemoryMappedFileFactory mmapFileFactory;
    private volatile ImmutableList<MMapQueueMessageSegment> segments = ImmutableList.of();

    public MMapTransactionalQueueService(Settings settings, Settings queueSettings, Queue queue) {
        super(settings, queueSettings, queue);
        workDir = this.settings.get("work_dir", "data");
        segmentMaxSize = this.settings.getAsInt("segment.max_size", MMapQueueMessageMetaSegment.DEFAULT_MAX_SIZE);
        segmentLoadFactor = this.settings.getAsFloat("segment.load_factor", MMapQueueMessageMetaSegment.DEFAULT_LOAD_FACTOR);
        transactionMap = new ConcurrentHashMap<>();
    }

    @Override
    protected void doStart() throws QueueException {
        segments = ImmutableList.of();
        try {
            queueDir = new File(workDir, queue.getName());
            if (!queueDir.exists() && !queueDir.mkdirs()) {
                throw new IOException("Error create dir " + queueDir);
            }
            mmapFileFactory = new MemoryMappedFileFactory(settings, queueDir);

            readState();
        } catch (IOException e) {
            throw new QueueException(e);
        }
    }

    private void readState() throws IOException {
        File state = new File(queueDir, "state.json");
        if (!state.exists()) return;

        logger.info("read state of segments");

        ImmutableList.Builder<MMapQueueMessageSegment> builder = ImmutableList.builder();
        try (FileInputStream inputStream = new FileInputStream(state)) {
            JsonFactory factory = new JsonFactory();
            JsonParser parser = factory.createParser(inputStream);

            assertEquals(JsonToken.START_OBJECT, parser.nextToken());
            assertEquals(JsonToken.FIELD_NAME, parser.nextToken());
            assertEquals("segments", parser.getCurrentName());
            assertEquals(JsonToken.START_ARRAY, parser.nextToken());

            JsonToken token;
            while ((token = parser.nextToken()) != JsonToken.END_ARRAY) {
                assertEquals(JsonToken.START_OBJECT, token);
                String appendFilePath = null;
                String metaFilePath = null;
                while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {
                    assertEquals(JsonToken.FIELD_NAME, token);
                    switch (parser.getCurrentName()) {
                        case "append":
                            assertEquals(JsonToken.VALUE_STRING, parser.nextToken());
                            appendFilePath = parser.getText();
                            break;
                        case "meta":
                            assertEquals(JsonToken.VALUE_STRING, parser.nextToken());
                            metaFilePath = parser.getText();
                            break;
                    }
                }
                if (appendFilePath == null) {
                    throw new QueueException("Queue append file not found");
                }
                if (metaFilePath == null) {
                    throw new QueueException("Queue meta file not found");
                }
                MemoryMappedFile appendFile = mmapFileFactory.createFile(new File(appendFilePath));
                MemoryMappedFile metaFile = mmapFileFactory.createFile(new File(metaFilePath));
                MMapQueueMessageAppendSegment appendSegment = createAppendSegment(appendFile);
                MMapQueueMessageMetaSegment metaSegment = createMetaSegment(metaFile);
                MMapQueueMessageSegment segment = new MMapQueueMessageSegment(
                    appendSegment,
                    metaSegment
                );
                builder.add(segment);
            }
            assertEquals(JsonToken.END_OBJECT, parser.nextToken());
            parser.close();

            segments = builder.build();
            logger.info("read state of {} segments done", segments.size());
        }
    }

    private <T> void assertEquals(T expected, T actual) {
        if (expected != actual) {
            throw new AssertionError("Expected " + expected + ", actual " + actual);
        }
    }

    private void writeState() throws IOException {
        ImmutableList<MMapQueueMessageSegment> segments = this.segments;
        logger.info("write state of {} segments", segments.size());
        long start = System.currentTimeMillis();
        File state = new File(queueDir, "state.json");
        if (!state.exists() && !state.createNewFile()) {
            throw new IOException("error create new file " + state);
        }

        try (FileOutputStream outputStream = new FileOutputStream(state)) {
            JsonFactory factory = new JsonFactory();
            JsonGenerator generator = factory.createGenerator(outputStream);
            generator.writeStartObject();

            generator.writeFieldName("segments");
            generator.writeStartArray();
            for (MMapQueueMessageSegment segment : segments) {
                generator.writeStartObject();
                generator.writeFieldName("append");
                generator.writeString(segment.getMessageAppendSegment().getMappedFile().getFile().getAbsolutePath());
                generator.writeFieldName("meta");
                generator.writeString(segment.getMessageMetaSegment().getMappedFile().getFile().getAbsolutePath());
                generator.writeEndObject();
            }
            generator.writeEndArray();
            // end write segments

            generator.writeEndObject();
            generator.close();
        } finally {
            long end = System.currentTimeMillis();
            logger.info("write state of {} segments done at {}ms", segments.size(), end - start);
        }
    }

    @Override
    protected void doStop() throws QueueException {
        for (MMapQueueMessageSegment segment : segments) {
            try {
                segment.close();
            } catch (IOException e) {
                throw new QueueException(e);
            }
        }
        segments = null;
        mmapFileFactory = null;
    }

    @Override
    protected void doClose() throws QueueException {
    }

    @Override
    public QueueTransaction transaction(UUID id) {
        return transactionMap.computeIfAbsent(id, MMapMemoryQueueTransaction::new);
    }

    @Override
    public QueueMessage get(UUID uuid) throws IOException {
        final ImmutableList<MMapQueueMessageSegment> current = this.segments;
        final int size = current.size();
        for (int i = 0; i < size; i++) {
            final MMapQueueMessageSegment segment = current.get(i);
            if (segment.acquire() > 0) {
                try {
                    QueueMessage queueMessage = segment.get(uuid);
                    if (queueMessage != null) {
                        return queueMessage;
                    }
                } finally {
                    segment.release();
                }
            }
        }
        return null;
    }

    @Override
    public QueueMessage lock(UUID uuid) throws IOException {
        final ImmutableList<MMapQueueMessageSegment> current = this.segments;
        final int size = current.size();
        for (int i = 0; i < size; i++) {
            final MMapQueueMessageSegment segment = current.get(i);
            if (segment.acquire() > 0) {
                try {
                    QueueMessage queueMessage = segment.lock(uuid);
                    if (queueMessage != null) {
                        return queueMessage;
                    }
                } finally {
                    segment.release();
                }
            }
        }
        return null;
    }

    @Override
    public QueueMessage peek() throws IOException {
        final ImmutableList<MMapQueueMessageSegment> current = this.segments;
        final int size = current.size();
        for (int i = 0; i < size; i++) {
            final MMapQueueMessageSegment segment = current.get(i);
            if (segment.acquire() > 0) {
                try {
                    QueueMessage queueMessage = segment.peek();
                    if (queueMessage != null) {
                        return queueMessage;
                    }
                } finally {
                    segment.release();
                }
            }
        }
        return null;
    }

    @Override
    public QueueMessage lockAndPop() throws IOException {
        final ImmutableList<MMapQueueMessageSegment> current = this.segments;
        final int size = current.size();
        for (int i = 0; i < size; i++) {
            final MMapQueueMessageSegment segment = current.get(i);
            if (segment.acquire() > 0) {
                try {
                    QueueMessage queueMessage = segment.lockAndPop();
                    if (queueMessage != null) {
                        return queueMessage;
                    }
                } finally {
                    segment.release();
                }
            }
        }
        return null;
    }

    @Override
    public QueueMessage unlockAndDelete(UUID uuid) throws IOException {
        final ImmutableList<MMapQueueMessageSegment> current = this.segments;
        final int size = current.size();
        for (int i = 0; i < size; i++) {
            final MMapQueueMessageSegment segment = current.get(i);
            if (segment.acquire() > 0) {
                try {
                    QueueMessage queueMessage = segment.unlockAndDelete(uuid);
                    if (queueMessage != null) {
                        return queueMessage;
                    }
                } finally {
                    segment.release();
                }
            }
        }
        return null;
    }

    @Override
    public QueueMessage unlockAndRollback(UUID uuid) throws IOException {
        final ImmutableList<MMapQueueMessageSegment> current = this.segments;
        final int size = current.size();
        for (int i = 0; i < size; i++) {
            final MMapQueueMessageSegment segment = current.get(i);
            if (segment.acquire() > 0) {
                try {
                    QueueMessage queueMessage = segment.unlockAndRollback(uuid);
                    if (queueMessage != null) {
                        return queueMessage;
                    }
                } finally {
                    segment.release();
                }
            }
        }
        return null;
    }

    @Override
    public boolean push(QueueMessage queueMessage) throws IOException {
        if (queueMessage.getUuid() == null) {
            return pushNew(queueMessage);
        } else {
            return pushExist(queueMessage);
        }
    }

    private boolean pushExist(QueueMessage queueMessage) throws IOException {
        final UUID uuid = queueMessage.getUuid();
        ImmutableList<MMapQueueMessageSegment> prev = null;
        while (true) {
            final ImmutableList<MMapQueueMessageSegment> current = this.segments;
            final int size = current.size();
            for (int i = 0; i < size; i++) {
                final MMapQueueMessageSegment segment = current.get(i);
                if (prev != null && prev.contains(segment)) {
                    continue;
                }
                if (segment.acquire() > 0) {
                    try {
                        int pos = segment.insert(uuid);
                        if (pos >= 0) {
                            if (segment.writeLock(pos)) {
                                segment.writeMessage(queueMessage, pos);
                                return true;
                            } else {
                                throw new QueueMessageUuidDuplicateException(uuid);
                            }
                        }
                    } finally {
                        segment.release();
                    }
                }
            }
            prev = current;
            addSegment(current);
        }
    }

    private boolean pushNew(QueueMessage queueMessage) throws IOException {
        final UUID uuid = UUIDs.generateRandom();
        queueMessage.setUuid(uuid);
        MMapQueueMessageSegment prev = null;
        while (true) {
            ImmutableList<MMapQueueMessageSegment> current = this.segments;
            if (!current.isEmpty()) {
                MMapQueueMessageSegment segment = current.get(current.size() - 1);
                if (prev != segment && segment.acquire() > 0) {
                    try {
                        int pos = segment.insert(uuid);
                        if (pos >= 0) {
                            if (segment.writeLock(pos)) {
                                segment.writeMessage(queueMessage, pos);
                                return true;
                            } else {
                                throw new QueueMessageUuidDuplicateException(uuid);
                            }
                        }
                    } finally {
                        segment.release();
                    }
                }
                prev = segment;
            }
            addSegment(current);
        }
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public void delete() throws IOException {

    }

    public int segmentsSize() {
        return segments.size();
    }

    public void garbageCollect() throws IOException {
        segmentsLock.lock();
        try {
            ImmutableList<MMapQueueMessageSegment> current = this.segments;
            ImmutableList.Builder<MMapQueueMessageSegment> garbageBuilder = ImmutableList.builder();
            ImmutableList.Builder<MMapQueueMessageSegment> cleanBuilder = ImmutableList.builder();

            for (MMapQueueMessageSegment segment : current) {
                if (segment.isGarbage() && segment.releaseGarbage()) {
                    garbageBuilder.add(segment);
                } else {
                    cleanBuilder.add(segment);
                }
            }

            this.segments = cleanBuilder.build();

            for (MMapQueueMessageSegment segment : garbageBuilder.build()) {
                segment.delete();
            }

        } finally {
            segmentsLock.unlock();
        }
    }

    private void addSegment(ImmutableList<MMapQueueMessageSegment> current) throws IOException {
        segmentsLock.lock();
        try {
            if (this.segments == current) {
                this.segments = ImmutableList.<MMapQueueMessageSegment>builder()
                    .addAll(current)
                    .add(createSegment())
                    .build();
                writeState();
            }
        } finally {
            segmentsLock.unlock();
        }
    }

    private MMapQueueMessageAppendSegment createAppendSegment() throws IOException {
        logger.info("create new data segment");
        long start = System.currentTimeMillis();
        MMapQueueMessageAppendSegment data = createAppendSegment(mmapFileFactory.createFile("data"));
        long end = System.currentTimeMillis();
        logger.info("create new data segment at {}ms", end - start);
        return data;
    }

    private MMapQueueMessageAppendSegment createAppendSegment(MemoryMappedFile file) throws IOException {
        return new MMapQueueMessageAppendSegment(file);
    }

    private MMapQueueMessageMetaSegment createMetaSegment() throws IOException {
        logger.info("create new meta segment");
        long start = System.currentTimeMillis();
        MMapQueueMessageMetaSegment meta = createMetaSegment(mmapFileFactory.createFile("meta"));
        long end = System.currentTimeMillis();
        logger.info("create new meta segment at {}ms", end - start);
        return meta;
    }

    private MMapQueueMessageMetaSegment createMetaSegment(MemoryMappedFile file) throws IOException {
        return new MMapQueueMessageMetaSegment(
            file,
            segmentMaxSize,
            segmentLoadFactor
        );
    }

    private MMapQueueMessageSegment createSegment() throws IOException {
        logger.info("create new segment");
        long start = System.currentTimeMillis();
        MMapQueueMessageSegment segment = new MMapQueueMessageSegment(
            createAppendSegment(),
            createMetaSegment()
        );
        long end = System.currentTimeMillis();
        logger.info("create new segment done at {}ms", end - start);
        return segment;
    }

    private static enum TransactionStatus {INIT, BEGIN, COMMIT, ROLLBACK}

    private static enum MessageStatus {PUSH, POP, DELETE}

    private class MMapMemoryQueueTransaction implements QueueTransaction {

        private final UUID id;
        private final ConcurrentHashMap<UUID, MessageStatus> messageStatusMap;
        private final ConcurrentHashMap<UUID, MMapQueueMessageSegment> messageSegmentMap;
        private final ConcurrentHashMap<UUID, QueueMessage> messageMap;
        private AtomicReference<TransactionStatus> status;

        public MMapMemoryQueueTransaction(UUID id) {
            this.id = id;
            this.messageStatusMap = new ConcurrentHashMap<>();
            this.messageSegmentMap = new ConcurrentHashMap<>();
            this.messageMap = new ConcurrentHashMap<>();
            this.status = new AtomicReference<>(TransactionStatus.BEGIN);
        }

        @Override
        public UUID id() throws IOException {
            return id;
        }

        @Override
        public void push(QueueMessage queueMessage) throws IOException {
            assertStatus(TransactionStatus.BEGIN);
            // optimized insert
            if (queueMessage.getUuid() == null) {
                insertNew(queueMessage);
            } else {
                insertExist(queueMessage);
            }

            messageStatusMap.put(queueMessage.getUuid(), MessageStatus.PUSH);
            messageMap.put(queueMessage.getUuid(), queueMessage);
        }

        private void insertNew(QueueMessage queueMessage) throws IOException {
            final UUID uuid = UUIDs.generateRandom();
            queueMessage.setUuid(uuid);
            MMapQueueMessageSegment prev = null;
            while (true) {
                ImmutableList<MMapQueueMessageSegment> current = segments;
                if (!current.isEmpty()) {
                    MMapQueueMessageSegment segment = current.get(current.size() - 1);
                    if (prev != segment && segment.acquire() > 0) {
                        try {
                            int pos = segment.insert(uuid);
                            if (pos >= 0) {
                                if (segment.writeLock(pos)) {
                                    messageSegmentMap.put(uuid, segment);
                                    return;
                                } else {
                                    throw new QueueMessageUuidDuplicateException(uuid);
                                }
                            }
                        } finally {
                            segment.release();
                        }
                    }
                    prev = segment;
                }
                addSegment(current);
            }
        }

        private void insertExist(QueueMessage queueMessage) throws IOException {
            final UUID uuid = queueMessage.getUuid();
            ImmutableList<MMapQueueMessageSegment> prev = null;
            while (true) {
                final ImmutableList<MMapQueueMessageSegment> current = segments;
                final int size = current.size();
                for (int i = 0; i < size; i++) {
                    final MMapQueueMessageSegment segment = current.get(i);
                    if (prev != null && prev.contains(segment)) {
                        continue;
                    }
                    if (segment.acquire() > 0) {
                        try {
                            int pos = segment.insert(uuid);
                            if (pos >= 0) {
                                if (segment.writeLock(pos)) {
                                    messageSegmentMap.put(uuid, segment);
                                    return;
                                } else {
                                    throw new QueueMessageUuidDuplicateException(uuid);
                                }
                            }
                        } finally {
                            segment.release();
                        }
                    }
                }
                prev = current;
                addSegment(current);
            }
        }

        @Override
        public QueueMessage pop() throws IOException {
            assertStatus(TransactionStatus.BEGIN);
            QueueMessage queueMessage = lockAndPop();
            messageStatusMap.put(queueMessage.getUuid(), MessageStatus.POP);
            messageMap.put(queueMessage.getUuid(), queueMessage);
            return queueMessage;
        }

        @Override
        public QueueMessage delete(UUID uuid) throws IOException {
            assertStatus(TransactionStatus.BEGIN);
            QueueMessage queueMessage = lock(uuid);
            messageStatusMap.put(uuid, MessageStatus.DELETE);
            messageMap.put(uuid, queueMessage);
            return queueMessage;
        }

        @Override
        public void commit() throws IOException {
            updateStatus(TransactionStatus.BEGIN, TransactionStatus.COMMIT);
            for (Map.Entry<UUID, MessageStatus> entry : messageStatusMap.entrySet()) {
                UUID uuid = entry.getKey();
                switch (entry.getValue()) {
                    case PUSH:
                        MMapQueueMessageSegment segment = messageSegmentMap.get(entry.getKey());
                        int pos = segment.insert(entry.getKey());
                        segment.writeMessage(messageMap.get(entry.getKey()), pos);
                        break;
                    case POP:
                        unlockAndDelete(uuid);
                        break;
                    case DELETE:
                        unlockAndDelete(uuid);
                        break;
                }
                messageStatusMap.remove(entry.getKey());
                messageMap.remove(entry.getKey());
            }
            transactionMap.remove(id);
        }

        @Override
        public void rollback() throws IOException {
            updateStatus(TransactionStatus.BEGIN, TransactionStatus.ROLLBACK);
            for (Map.Entry<UUID, MessageStatus> entry : messageStatusMap.entrySet()) {
                switch (entry.getValue()) {
                    case PUSH:
                        // ignore
                        MMapQueueMessageSegment segment = messageSegmentMap.get(entry.getKey());
                        segment.unlockAndDelete(entry.getKey());
                        break;
                    case POP:
                        unlockAndRollback(entry.getKey());
                        break;
                    case DELETE:
                        unlockAndRollback(entry.getKey());
                        break;
                }
                messageStatusMap.remove(entry.getKey());
                messageMap.remove(entry.getKey());
            }
            transactionMap.remove(id);

        }

        private void updateStatus(TransactionStatus expected, TransactionStatus update) throws IOException {
            if (!status.compareAndSet(expected, update)) {
                throw new IOException("Expected status [" + expected + "], actual " + status.get());
            }
        }

        private void assertStatus(TransactionStatus expected) throws IOException {
            if (status.get() != expected) {
                throw new IOException("Expected status [" + expected + "], actual " + status.get());
            }
        }
    }
}
