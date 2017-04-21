package org.mitallast.queue.raft.persistent;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import javaslang.collection.Vector;
import javaslang.control.Option;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mitallast.queue.common.file.FileService;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.StreamService;
import org.mitallast.queue.raft.protocol.LogEntry;
import org.mitallast.queue.raft.protocol.RaftSnapshot;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.File;

public class FilePersistentService implements PersistentService {
    private final static Logger logger = LogManager.getLogger();

    private final static long initialIndex = 1;
    private final static long initialCommittedIndex = 0;

    private final FileService fileService;
    private final StreamService streamService;
    private final File stateFile;

    private long segment;
    private long currentTerm;
    private Option<DiscoveryNode> votedFor;

    @Inject
    public FilePersistentService(FileService fileService, StreamService streamService) {
        this.fileService = fileService;
        this.streamService = streamService;
        this.stateFile = fileService.resource("raft", "state.bin");
        readState();
    }

    private void readState() {
        if (stateFile.length() == 0) {
            segment = initialIndex;
            currentTerm = 0;
            votedFor = Option.none();
            writeState();
            logger.info("initialize state: segment={} term={} voted={}", segment, currentTerm, votedFor);
        } else {
            try (StreamInput input = streamService.input(stateFile)) {
                segment = input.readLong();
                currentTerm = input.readLong();
                votedFor = input.readOpt(DiscoveryNode::new);
                logger.info("read state: segment={} term={} voted={}", segment, currentTerm, votedFor);
            }
        }
    }

    private void writeState() {
        try (StreamOutput output = streamService.output(stateFile)) {
            logger.info("write state: segment={} term={} voted={}", segment, currentTerm, votedFor);
            output.writeLong(segment);
            output.writeLong(currentTerm);
            output.writeOpt(votedFor);
        }
    }

    @Override
    public long currentTerm() {
        return currentTerm;
    }

    @Override
    public Option<DiscoveryNode> votedFor() {
        return votedFor;
    }

    @Override
    public void updateState(long newTerm, Option<DiscoveryNode> node) {
        boolean update = false;
        if (currentTerm != newTerm) {
            currentTerm = newTerm;
            update = true;
        }
        if (!votedFor.equals(node)) {
            votedFor = node;
            update = true;
        }
        if (update) {
            writeState();
        }
    }

    private void updateSegment(long segment) {
        if (this.segment != segment) {
            this.segment = segment;
            writeState();
        }
    }

    @Override
    public ReplicatedLog openLog() {
        logger.info("open log: segment={}", segment);
        final File segmentFile = segmentFile(segment);
        Vector<LogEntry> entries = Vector.empty();
        try (StreamInput input = streamService.input(segmentFile)) {
            while (input.available() > 0) {
                entries = entries.append(input.readStreamable(LogEntry::new));
            }
        }
        StreamOutput segmentOutput = streamService.output(segmentFile, true);
        return new FileReplicatedLog(
            segmentFile,
            segmentOutput,
            entries,
            initialCommittedIndex,
            segment
        );
    }

    private File segmentFile(long segment) {
        return fileService.resource("raft", segment + ".log");
    }

    private File temporaryFile() {
        return fileService.temporary("raft", "log.", ".tmp");
    }

    public class FileReplicatedLog implements ReplicatedLog {
        private volatile Vector<LogEntry> entries;
        private volatile boolean dirty = false;
        private volatile long start;

        private volatile File segmentFile;
        private volatile StreamOutput segmentOutput;

        private volatile long committedIndex;

        public FileReplicatedLog(
            File segmentFile,
            StreamOutput segmentOutput,
            Vector<LogEntry> entries,
            long committedIndex, long start
        ) {
            this.entries = entries;
            this.committedIndex = committedIndex;
            this.start = start;
            this.segmentFile = segmentFile;
            this.segmentOutput = segmentOutput;
        }

        @Override
        public boolean isEmpty() {
            return entries.isEmpty();
        }

        @Override
        public boolean contains(LogEntry entry) {
            return entries.contains(entry);
        }

        @Override
        public Vector<LogEntry> entries() {
            return entries;
        }

        @Override
        public int committedEntries() {
            return (int) (committedIndex - start + 1);
        }

        @Override
        public long committedIndex() {
            return committedIndex;
        }

        private long offset() {
            return start - 1;
        }

        private long length() {
            return entries.size() + offset();
        }

        private LogEntry get(long index) {
            return entries.get((int) (index - start));
        }

        private LogEntry last() {
            return entries.get(entries.size() - 1);
        }

        @Override
        public boolean containsMatchingEntry(long otherPrevTerm, long otherPrevIndex) {
            return (otherPrevTerm == 0 && otherPrevIndex == 0) ||
                (!isEmpty() && otherPrevIndex >= committedIndex() && containsEntryAt(otherPrevIndex) && termAt(otherPrevIndex) == otherPrevTerm);
        }

        @Override
        public Option<Long> lastTerm() {
            return (entries.isEmpty()) ? Option.none() : Option.some(last().term());
        }

        @Override
        public long lastIndex() {
            return entries.isEmpty() ? 1 : last().index();
        }

        @Override
        public long prevIndex() {
            return Math.max(0, lastIndex() - 1);
        }

        @Override
        public long nextIndex() {
            return entries.isEmpty() ? 1 : last().index() + 1;
        }

        @Override
        public ReplicatedLog commit(long committedIndex) {
            Preconditions.checkArgument(this.committedIndex <= committedIndex, "commit index cannot be less than current commit");
            Preconditions.checkArgument(lastIndex() >= committedIndex, "commit index cannot be greater than last index");
            this.committedIndex = committedIndex;
            flush();
            return this;
        }

        @Override
        public ReplicatedLog append(LogEntry entry) {
            Preconditions.checkArgument(entry.index() > committedIndex, "entry index should be > committed index");
            Preconditions.checkArgument(entry.index() >= start, "entry index should be >= start index");

            if (entry.index() <= length()) { // if contains
                LogEntry contains = get(entry.index());
                if (contains.term() == entry.term()) { // if term matches, entry already contains in log
                    return this;
                } else {
                    long prev = entry.index() - 1;
                    truncate(prev);
                }
            }

            dirty = true;
            segmentOutput.writeStreamable(entry);
            entries = entries.append(entry);
            return this;
        }

        @Override
        public ReplicatedLog append(Vector<LogEntry> entries) {
            for (LogEntry entry : entries) {
                append(entry);
            }
            return this;
        }

        /**
         * truncate index exclusive truncate index
         */
        private void truncate(long truncateIndex) {
            Preconditions.checkArgument(truncateIndex >= committedIndex, "truncate index should be > committed index %d", committedIndex);
            Preconditions.checkArgument(truncateIndex < lastIndex(), "truncate index should be < last index");

            entries = entries.dropRightUntil(entry -> entry.index() <= truncateIndex);

            segmentOutput.close();
            File tmpSegment = temporaryFile();
            try (StreamOutput output = streamService.output(tmpSegment, false)) {
                for (LogEntry logEntry : entries) {
                    output.writeStreamable(logEntry);
                }
            }
            fileService.move(tmpSegment, segmentFile);

            // recreate file object after move
            this.segmentFile = segmentFile(start);
            this.segmentOutput = streamService.output(this.segmentFile);
            dirty = false;
        }

        @Override
        public Vector<LogEntry> entriesBatchFrom(long fromIncluding, int howMany) {
            Vector<LogEntry> toSend = slice(fromIncluding, fromIncluding + howMany);
            if (toSend.isEmpty()) {
                return toSend;
            } else {
                long batchTerm = toSend.get(0).term();
                Vector<LogEntry> builder = Vector.empty();
                for (LogEntry logEntry : toSend) {
                    if (logEntry.term() == batchTerm) {
                        builder = builder.append(logEntry);
                    } else {
                        break;
                    }
                }
                return builder;
            }
        }

        @Override
        public Vector<LogEntry> slice(long from, long until) {
            int fromIndex = (int) (from - start);
            int toIndex = (int) (until - start + 1);
            if (fromIndex >= entries.size()) {
                return Vector.empty();
            }
            return entries.subSequence(Math.max(0, fromIndex), Math.min(toIndex, entries.size()));
        }

        @Override
        public boolean containsEntryAt(long index) {
            return index >= start && index <= length() && get(index).index() == index;
        }

        @Override
        public long termAt(long index) {
            if (index <= 0) {
                return 0;
            } else if (!containsEntryAt(index)) {
                throw new IllegalArgumentException("Unable to find log entry at index " + index);
            } else {
                return get(index).term();
            }
        }

        @Override
        public ReplicatedLog compactWith(RaftSnapshot snapshot) {
            long lastIncludedIndex = snapshot.getMeta().getLastIncludedIndex();
            LogEntry snapshotEntry = snapshot.toEntry();

            if (entries.isEmpty()) {
                entries = Vector.of(snapshotEntry);
            } else {
                if (entries.get(0).index() > lastIncludedIndex) {
                    throw new IllegalArgumentException("snapshot too old");
                }
                entries = entries.dropUntil(entry -> entry.index() > lastIncludedIndex).prepend(snapshotEntry);
            }

            if (snapshot.getMeta().getLastIncludedIndex() == start) {
                segmentOutput.close();
                File tmpSegment = temporaryFile();
                try (StreamOutput output = streamService.output(tmpSegment, false)) {
                    for (LogEntry logEntry : entries) {
                        output.writeStreamable(logEntry);
                    }
                }
                fileService.move(tmpSegment, segmentFile);

                // recreate file object after move
                segmentFile = segmentFile(start);
                segmentOutput = streamService.output(segmentFile);
                dirty = false;
                return this;
            } else {
                segmentOutput.close();
                File newSegmentFile = segmentFile(lastIncludedIndex);
                StreamOutput newSegmentOutput = streamService.output(newSegmentFile);
                for (LogEntry logEntry : entries) {
                    newSegmentOutput.writeStreamable(logEntry);
                }
                newSegmentOutput.flush();
                updateSegment(snapshot.getMeta().getLastIncludedIndex());

                fileService.delete(segmentFile);

                this.segmentFile = newSegmentFile;
                this.segmentOutput = newSegmentOutput;
                this.start = lastIncludedIndex;
                dirty = false;
                return this;
            }
        }

        @Override
        public boolean hasSnapshot() {
            return !entries.isEmpty() && entries.get(0).command() instanceof RaftSnapshot;
        }

        @Override
        public RaftSnapshot snapshot() {
            return (RaftSnapshot) entries.get(0).command();
        }

        private void flush() {
            if (dirty) {
                segmentOutput.flush();
                dirty = false;
            }
        }

        @Override
        public void close() {
            flush();
            segmentOutput.close();

            segmentFile = null;
            segmentOutput = null;
        }

        @Override
        public String toString() {
            return "ReplicatedLog{" +
                "entries=" + entries +
                ", committedIndex=" + committedIndex +
                ", start=" + start +
                ", file=" + segmentFile.toPath().getFileName() + " (" + segmentFile.length() + " bytes)" +
                '}';
        }
    }
}
