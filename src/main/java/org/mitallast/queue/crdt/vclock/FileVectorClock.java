package org.mitallast.queue.crdt.vclock;

import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import gnu.trove.iterator.TObjectLongIterator;
import gnu.trove.map.TObjectLongMap;
import gnu.trove.map.hash.TObjectLongHashMap;
import org.mitallast.queue.common.file.FileService;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.StreamService;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.locks.ReentrantLock;

public class FileVectorClock implements VectorClock {
    private final FileService fileService;
    private final StreamService streamService;
    private final TObjectLongMap<DiscoveryNode> vclock;
    private final ReentrantLock writeLock = new ReentrantLock();
    private final String serviceName;

    private volatile File vclockFile;
    private volatile StreamOutput vclockOutput;
    private volatile int logSize;

    @Inject
    public FileVectorClock(
        FileService fileService,
        StreamService streamService,
        @Assisted int index
    ) throws IOException {
        this.fileService = fileService;
        this.streamService = streamService;
        this.vclock = new TObjectLongHashMap<>(7, 0.5f, 0);
        this.logSize = 0;
        this.serviceName = String.format("crdt/%d/vclock", index);

        this.vclockFile = fileService.resource(serviceName, "vclock.log");
        this.vclockOutput = streamService.output(vclockFile, true);

        if (vclockFile.length() > 0) {
            try (StreamInput stream = streamService.input(vclockFile)) {
                while (stream.available() > 0) {
                    DiscoveryNode node = stream.readStreamable(DiscoveryNode::new);
                    long nodeVclock = stream.readLong();
                    vclock.put(node, nodeVclock);
                    logSize++;
                }
            }
        }
    }

    @Override
    public void put(DiscoveryNode node, long nodeVclock) throws IOException {
        writeLock.lock();
        try {
            vclock.put(node, nodeVclock);
            vclockOutput.writeStreamable(node);
            vclockOutput.writeLong(nodeVclock);
            logSize++;
            if (logSize > vclock.size() + 1000000) {
                vclockOutput.close();

                File tmp = fileService.temporary(serviceName, "vclock", "log");
                try (StreamOutput stream = streamService.output(tmp)) {
                    TObjectLongIterator<DiscoveryNode> iterator = vclock.iterator();
                    while (iterator.hasNext()) {
                        iterator.advance();
                        stream.writeStreamable(iterator.key());
                        stream.writeLong(iterator.value());
                    }
                }

                Files.move(tmp.toPath(), vclockFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

                this.vclockFile = fileService.resource(serviceName, "vclock.log");
                this.vclockOutput = streamService.output(vclockFile, true);
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public long get(DiscoveryNode node) {
        return vclock.get(node);
    }

    @Override
    public TObjectLongMap<DiscoveryNode> getAll() {
        return vclock;
    }

    @Override
    public void close() throws IOException {
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
    public void delete() throws IOException {
        close();
        fileService.delete(serviceName);
    }
}
