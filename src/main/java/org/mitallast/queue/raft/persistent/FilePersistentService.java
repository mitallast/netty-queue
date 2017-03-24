package org.mitallast.queue.raft.persistent;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.protobuf.*;
import com.typesafe.config.Config;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.file.FileService;
import org.mitallast.queue.common.proto.ProtoService;
import org.mitallast.queue.proto.raft.DiscoveryNode;
import org.mitallast.queue.proto.raft.LogEntry;
import org.mitallast.queue.proto.raft.RaftSnapshot;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class FilePersistentService extends AbstractComponent implements PersistentService {

    private final static long initialIndex = 1;
    private final static long initialCommittedIndex = 0;

    private final FileService fileService;
    private final ProtoService protoService;
    private final File stateFile;

    private long segment;
    private long currentTerm;
    private Optional<DiscoveryNode> votedFor;

    @Inject
    public FilePersistentService(Config config, FileService fileService, ProtoService protoService) throws IOException {
        super(config, PersistentService.class);
        this.protoService = protoService;
        this.fileService = fileService;
        this.stateFile = fileService.resource("raft", "state.bin");
        readState();
    }

    private void readState() throws IOException {
        if (stateFile.length() == 0) {
            segment = initialIndex;
            currentTerm = 0;
            votedFor = Optional.empty();
            writeState();
            if (logger.isInfoEnabled()) {
                logger.info("initialize state: segment={} term={} voted={}", segment, currentTerm, votedFor.map(TextFormat::shortDebugString));
            }
        } else {
            try (FileInputStream input = new FileInputStream(stateFile);
                 DataInputStream data = new DataInputStream(input)
            ) {
                segment = data.readLong();
                currentTerm = data.readLong();
                if (data.readBoolean()) {
                    votedFor = Optional.of(DiscoveryNode.parseFrom(input));
                } else {
                    votedFor = Optional.empty();
                }
                if (logger.isInfoEnabled()) {
                    logger.info("read state: segment={} term={} voted={}", segment, currentTerm, votedFor.map(TextFormat::shortDebugString));
                }
            }
        }
    }

    private void writeState() throws IOException {
        try (FileOutputStream output = new FileOutputStream(stateFile);
             DataOutputStream data = new DataOutputStream(output)
        ) {

            if (logger.isInfoEnabled()) {
                logger.info("write state: segment={} term={} voted={}", segment, currentTerm, votedFor.map(TextFormat::shortDebugString));
            }
            data.writeLong(segment);
            data.writeLong(currentTerm);
            data.writeBoolean(votedFor.isPresent());
            if (votedFor.isPresent()) {
                votedFor.get().writeTo(output);
            }
        }
    }

    @Override
    public long currentTerm() {
        return currentTerm;
    }

    @Override
    public Optional<DiscoveryNode> votedFor() {
        return votedFor;
    }

    @Override
    public void updateState(long newTerm, Optional<DiscoveryNode> node) throws IOException {
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

    private void updateSegment(long segment) throws IOException {
        if (this.segment != segment) {
            this.segment = segment;
            writeState();
        }
    }

    @Override
    public ReplicatedLog openLog() throws IOException {
        logger.info("open log: segment={}", segment);
        final File segmentFile = segmentFile(segment);
        ArrayList<LogEntry> entries = new ArrayList<>();

        try (FileInputStream input = new FileInputStream(segmentFile)) {
            CodedInputStream coded = CodedInputStream.newInstance(input);
            while (!coded.isAtEnd()) {
                entries.add(protoService.readDelimited(coded, LogEntry.parser()));
            }
        }

        FileOutputStream output = new FileOutputStream(segmentFile, true);
        CodedOutputStream coded = CodedOutputStream.newInstance(output, 65536);
        return new FileReplicatedLog(
            segmentFile,
            output,
            coded,
            entries,
            initialCommittedIndex,
            segment
        );
    }

    private File segmentFile(long segment) throws IOException {
        return fileService.resource("raft", segment + ".log");
    }

    private File temporaryFile() throws IOException {
        return fileService.temporary("raft", "log.", ".tmp");
    }

    public class FileReplicatedLog implements ReplicatedLog {
        private final ArrayList<LogEntry> entries;
        private boolean dirty = false;
        private long start;

        private File segmentFile;
        private FileOutputStream output;
        private CodedOutputStream coded;

        private long committedIndex;

        public FileReplicatedLog(File segmentFile, FileOutputStream output, CodedOutputStream coded, ArrayList<LogEntry> entries, long committedIndex, long start) throws IOException {
            this.entries = entries;
            this.committedIndex = committedIndex;
            this.start = start;
            this.segmentFile = segmentFile;
            this.output = output;
            this.coded = coded;
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
        public ImmutableList<LogEntry> entries() {
            return ImmutableList.copyOf(entries);
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
            return (otherPrevTerm == 0 && otherPrevIndex == 0 && entries.isEmpty()) ||
                (!isEmpty() && otherPrevIndex >= committedIndex() && containsEntryAt(otherPrevIndex) && termAt(otherPrevIndex) == otherPrevTerm);
        }

        @Override
        public Optional<Long> lastTerm() {
            return (entries.isEmpty()) ? Optional.empty() : Optional.of(last().getTerm());
        }

        @Override
        public long lastIndex() {
            return (entries.isEmpty()) ? 1 : last().getIndex();
        }

        @Override
        public long prevIndex() {
            return Math.max(0, lastIndex() - 1);
        }

        @Override
        public long nextIndex() {
            return entries.isEmpty() ? 1 : last().getIndex() + 1;
        }

        @Override
        public ReplicatedLog commit(long committedIndex) throws IOException {
            this.committedIndex = committedIndex;
            flush();
            return this;
        }

        @Override
        public ReplicatedLog append(LogEntry entry) throws IOException {
            dirty = true;
            protoService.writeDelimited(coded, entry);
            this.entries.add(entry);
            return this;
        }

        @Override
        public ReplicatedLog append(List<LogEntry> entries) throws IOException {
            dirty = true;
            for (LogEntry entry : entries) {
                protoService.writeDelimited(coded, entry);
            }
            this.entries.addAll(entries);
            return this;
        }

        @Override
        public ReplicatedLog append(List<LogEntry> append, long prevIndex) throws IOException {
            if (prevIndex == lastIndex()) {
                return append(append);
            } else {
                for (int i = entries.size() - 1; i >= 0; i--) {
                    if (entries.get(i).getIndex() > prevIndex) {
                        entries.remove(i);
                    }
                }
                entries.addAll(append);
                output.close();

                File tmpSegment = temporaryFile();
                try (FileOutputStream output = new FileOutputStream(tmpSegment, true)) {
                    CodedOutputStream coded = CodedOutputStream.newInstance(output, 65536);
                    for (LogEntry logEntry : entries) {
                        protoService.writeDelimited(coded, logEntry);
                    }
                    coded.flush();
                }

                Files.move(tmpSegment.toPath(), segmentFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

                // recreate file object after move
                File newSegmentFile = segmentFile(start);
                FileOutputStream output = new FileOutputStream(newSegmentFile, true);
                CodedOutputStream coded = CodedOutputStream.newInstance(output, 65536);

                this.segmentFile = newSegmentFile;
                this.output = output;
                this.coded = coded;
                dirty = false;
                return this;
            }
        }

        @Override
        public ImmutableList<LogEntry> entriesBatchFrom(long fromIncluding, int howMany) {
            ImmutableList<LogEntry> toSend = slice(fromIncluding, fromIncluding + howMany);
            if (toSend.isEmpty()) {
                return toSend;
            } else {
                long batchTerm = toSend.get(0).getTerm();
                ImmutableList.Builder<LogEntry> builder = ImmutableList.builder();
                for (LogEntry logEntry : toSend) {
                    if (logEntry.getTerm() == batchTerm) {
                        builder.add(logEntry);
                    } else {
                        break;
                    }
                }
                return builder.build();
            }
        }

        @Override
        public ImmutableList<LogEntry> slice(long from, long until) {
            int fromIndex = (int) (from - start);
            int toIndex = (int) (until - start + 1);
            if (fromIndex >= entries.size()) {
                return ImmutableList.of();
            }
            return ImmutableList.copyOf(entries.subList(Math.max(0, fromIndex), Math.min(toIndex, entries.size())));
        }

        @Override
        public boolean containsEntryAt(long index) {
            return index >= start && index <= length() && get(index).getIndex() == index;
        }

        @Override
        public long termAt(long index) {
            if (index <= 0) {
                return 0;
            } else if (!containsEntryAt(index)) {
                throw new IllegalArgumentException("Unable to find log entry at index " + index);
            } else {
                return get(index).getTerm();
            }
        }

        @Override
        public ReplicatedLog compactWith(RaftSnapshot snapshot, DiscoveryNode node) throws IOException {
            long lastIncludedIndex = snapshot.getMeta().getLastIncludedIndex();
            LogEntry snapshotEntry = LogEntry.newBuilder()
                .setCommand(protoService.pack(snapshot))
                .setTerm(snapshot.getMeta().getLastIncludedTerm())
                .setIndex(snapshot.getMeta().getLastIncludedIndex())
                .setClient(node)
                .build();

            if (entries.isEmpty()) {
                entries.add(snapshotEntry);
            } else {
                if (entries.get(0).getIndex() <= lastIncludedIndex) {
                    entries.set(0, snapshotEntry);
                } else {
                    throw new IllegalArgumentException("snapshot too old");
                }
                entries.removeIf(entry -> entry != snapshotEntry && entry.getIndex() <= lastIncludedIndex);
            }

            if (snapshot.getMeta().getLastIncludedIndex() == start) {
                output.close();
                File tmpSegment = temporaryFile();
                try (FileOutputStream output = new FileOutputStream(tmpSegment, true)) {
                    CodedOutputStream coded = CodedOutputStream.newInstance(output, 65536);
                    for (LogEntry logEntry : entries) {
                        protoService.writeDelimited(coded, logEntry);
                    }
                    coded.flush();
                }
                Files.move(tmpSegment.toPath(), segmentFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

                // recreate file object after move
                File newSegmentFile = segmentFile(start);
                FileOutputStream output = new FileOutputStream(newSegmentFile, true);
                CodedOutputStream coded = CodedOutputStream.newInstance(output, 65536);

                this.segmentFile = newSegmentFile;
                this.output = output;
                this.coded = coded;
                this.start = lastIncludedIndex;
                dirty = false;
                return this;
            } else {
                output.close();

                File newSegmentFile = segmentFile(snapshot.getMeta().getLastIncludedIndex());
                FileOutputStream output = new FileOutputStream(newSegmentFile, true);
                CodedOutputStream coded = CodedOutputStream.newInstance(output, 65536);

                for (LogEntry logEntry : entries) {
                    protoService.writeDelimited(coded, logEntry);
                }
                coded.flush();
                output.flush();
                updateSegment(snapshot.getMeta().getLastIncludedIndex());

                Files.delete(segmentFile.toPath());

                this.segmentFile = newSegmentFile;
                this.output = output;
                this.coded = coded;
                this.start = lastIncludedIndex;
                dirty = false;
                return this;
            }
        }

        @Override
        public boolean hasSnapshot() {
            return !entries.isEmpty() && protoService.is(entries.get(0).getCommand(), RaftSnapshot.getDescriptor());
        }

        @Override
        public RaftSnapshot snapshot() {
            return protoService.unpack(entries.get(0).getCommand(), RaftSnapshot.parser());
        }

        private void flush() throws IOException {
            if (dirty) {
                coded.flush();
                output.flush();
                dirty = false;
            }
        }

        @Override
        public void close() throws IOException {
            flush();
            output.flush();
            output.close();

            segmentFile = null;
            output = null;
            coded = null;
        }

        @Override
        public String toString() {
            return "ReplicatedLog{" +
                "entries=" + entries +
                ", committedIndex=" + committedIndex +
                ", start=" + start +
                ", file=" + segmentFile + " (" + segmentFile.length() + " bytes)" +
                '}';
        }
    }
}
