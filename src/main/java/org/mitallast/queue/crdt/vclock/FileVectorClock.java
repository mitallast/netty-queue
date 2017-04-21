package org.mitallast.queue.crdt.vclock;

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
import java.util.concurrent.locks.ReentrantLock;

public class FileVectorClock implements VectorClock {
    private final FileService fileService;
    private final StreamService streamService;
    private final TLongLongMap vclock;
    private final ReentrantLock writeLock = new ReentrantLock();
    private final String serviceName;

    private volatile File vclockFile;
    private volatile StreamOutput vclockOutput;
    private volatile int logSize;

    @Inject
    public FileVectorClock(
        FileService fileService,
        StreamService streamService,
        @Assisted int index,
        @Assisted long replicaId
    ) {
        this.fileService = fileService;
        this.streamService = streamService;
        this.vclock = new TSynchronizedLongLongMap(new TLongLongHashMap(7, 0.5f, 0, 0));
        this.logSize = 0;
        this.serviceName = String.format("crdt/%d/vclock/%d", index, replicaId);

        this.vclockFile = fileService.resource(serviceName, "vclock.log");
        this.vclockOutput = streamService.output(vclockFile, true);

        if (vclockFile.length() > 0) {
            try (StreamInput stream = streamService.input(vclockFile)) {
                while (stream.available() > 0) {
                    long replica = stream.readLong();
                    long nodeVclock = stream.readLong();
                    vclock.put(replica, nodeVclock);
                    logSize++;
                }
            }
        }
    }

    @Override
    public void put(long replica, long nodeVclock) {
        writeLock.lock();
        try {
            assert vclock.get(replica) <= nodeVclock;
            vclock.put(replica, nodeVclock);
            vclockOutput.writeLong(replica);
            vclockOutput.writeLong(nodeVclock);
            logSize++;
            if (logSize > vclock.size() + 1000000) {
                vclockOutput.close();

                File tmp = fileService.temporary(serviceName, "vclock", "log");
                try (StreamOutput stream = streamService.output(tmp)) {
                    TLongLongIterator iterator = vclock.iterator();
                    while (iterator.hasNext()) {
                        iterator.advance();
                        stream.writeLong(iterator.key());
                        stream.writeLong(iterator.value());
                    }
                }

                fileService.move(tmp, vclockFile);

                this.vclockFile = fileService.resource(serviceName, "vclock.log");
                this.vclockOutput = streamService.output(vclockFile, true);
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public long get(long replica) {
        return vclock.get(replica);
    }

    @Override
    public void close() {
        writeLock.lock();
        try {
            vclockFile = null;
            if (vclockOutput != null) {
                vclockOutput.close();
                vclockOutput = null;
            }
            logSize = 0;
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
