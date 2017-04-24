package org.mitallast.queue.crdt.replication.state;

import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import gnu.trove.impl.sync.TSynchronizedLongLongMap;
import gnu.trove.iterator.TLongLongIterator;
import gnu.trove.map.TLongLongMap;
import gnu.trove.map.hash.TLongLongHashMap;
import org.mitallast.queue.common.file.FileService;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.StreamService;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class FileReplicaState implements ReplicaState {
    private final FileService fileService;
    private final StreamService streamService;
    private final TLongLongMap indexMap;
    private final AtomicInteger size;
    private final ReentrantLock writeLock;
    private final String serviceName;

    private volatile File file;
    private volatile StreamOutput output;

    @Inject
    public FileReplicaState(
        FileService fileService,
        StreamService streamService,
        @Assisted int index,
        @Assisted long replicaId
    ) {
        this.fileService = fileService;
        this.streamService = streamService;
        this.indexMap = new TSynchronizedLongLongMap(new TLongLongHashMap(7, 0.5f, 0, 0));
        this.size = new AtomicInteger();
        this.writeLock = new ReentrantLock();
        this.serviceName = String.format("crdt/%d/replica/%d", index, replicaId);

        this.file = fileService.resource(serviceName, "state.log");
        this.output = streamService.output(file, true);

        if (file.length() > 0) {
            try (StreamInput stream = streamService.input(file)) {
                while (stream.available() > 0) {
                    indexMap.put(stream.readLong(), stream.readLong());
                    size.incrementAndGet();
                }
            }
        }
    }

    @Override
    public void put(long replica, long logIndex) {
        writeLock.lock();
        try {
            assert indexMap.get(replica) <= logIndex;
            indexMap.put(replica, logIndex);
            output.writeLong(replica);
            output.writeLong(logIndex);
            int size = this.size.incrementAndGet();
            if (size > indexMap.size() + 1000000) {
                output.close();

                File tmp = fileService.temporary(serviceName, "state", "log");
                try (StreamOutput stream = streamService.output(tmp)) {
                    TLongLongIterator iterator = indexMap.iterator();
                    while (iterator.hasNext()) {
                        iterator.advance();
                        stream.writeLong(iterator.key());
                        stream.writeLong(iterator.value());
                    }
                }

                fileService.move(tmp, file);

                this.file = fileService.resource(serviceName, "state.log");
                this.output = streamService.output(file, true);
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public long get(long replica) {
        return indexMap.get(replica);
    }

    @Override
    public void close() {
        writeLock.lock();
        try {
            file = null;
            if (output != null) {
                output.close();
                output = null;
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void delete() {
        close();
        fileService.delete(serviceName);
    }
}
