package org.mitallast.queue.raft.persistent;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.file.FileService;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.StreamService;
import org.mitallast.queue.raft.Term;
import org.mitallast.queue.raft.protocol.LogEntry;
import org.mitallast.queue.raft.protocol.RaftSnapshot;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Optional;

public class FilePersistentService extends AbstractComponent implements PersistentService {

    private final static long initialIndex = 1;
    private final static long initialCommittedIndex = 0;

    private final FileService fileService;
    private final StreamService streamService;
    private final File stateFile;

    private long segment;
    private Term currentTerm;
    private Optional<DiscoveryNode> votedFor;

    @Inject
    public FilePersistentService(Config config, FileService fileService, StreamService streamService) throws IOException {
        super(config, PersistentService.class);
        this.fileService = fileService;
        this.streamService = streamService;
        this.stateFile = fileService.resource("raft", "state.bin");
        readState();
    }

    private void readState() throws IOException {
        if (stateFile.length() == 0) {
            segment = initialIndex;
            currentTerm = new Term(0);
            votedFor = Optional.empty();
            writeState();
            logger.info("initialize state: segment={} term={} voted={}", segment, currentTerm, votedFor);
        } else {
            try (StreamInput input = streamService.input(stateFile)) {
                segment = input.readLong();
                currentTerm = new Term(input.readLong());
                votedFor = Optional.ofNullable(input.readStreamableOrNull(DiscoveryNode::new));
                logger.info("read state: segment={} term={} voted={}", segment, currentTerm, votedFor);
            }
        }
    }

    private void writeState() throws IOException {
        try (StreamOutput output = streamService.output(stateFile)) {
            logger.info("write state: segment={} term={} voted={}", segment, currentTerm, votedFor);
            output.writeLong(segment);
            output.writeLong(currentTerm.getTerm());
            output.writeStreamableOrNull(votedFor.orElse(null));
        }
    }

    @Override
    public Term currentTerm() {
        return currentTerm;
    }

    @Override
    public Optional<DiscoveryNode> votedFor() {
        return votedFor;
    }

    @Override
    public void updateState(Term newTerm, Optional<DiscoveryNode> node) throws IOException {
        boolean update = false;
        if (!currentTerm.equals(newTerm)) {
            currentTerm = newTerm;
            update = true;
        }
        if (!votedFor.equals(node)) {
            votedFor = node;
            update = true;
        }
        if(update) {
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
        try (StreamInput input = streamService.input(segmentFile)) {
            while (input.available() > 0) {
                entries.add(input.readStreamable(LogEntry::new));
            }
        }
        BufferedOutputStream buffered = new BufferedOutputStream(new FileOutputStream(segmentFile, true), 65536);
        StreamOutput segmentOutput = streamService.output(buffered);
        return new FileReplicatedLog(
            segmentFile,
            buffered,
            segmentOutput,
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
        private BufferedOutputStream buffered;
        private StreamOutput segmentOutput;

        private long committedIndex;

        public FileReplicatedLog(File segmentFile, BufferedOutputStream buffered, StreamOutput segmentOutput, ArrayList<LogEntry> entries, long committedIndex, long start) throws IOException {
            this.entries = entries;
            this.committedIndex = committedIndex;
            this.start = start;
            this.segmentFile = segmentFile;
            this.buffered = buffered;
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
        public boolean containsMatchingEntry(Term otherPrevTerm, long otherPrevIndex) {
            return (otherPrevTerm.getTerm() == 0 && otherPrevIndex == 0 && entries.isEmpty()) ||
                (!isEmpty() && otherPrevIndex >= committedIndex() && containsEntryAt(otherPrevIndex) && termAt(otherPrevIndex).equals(otherPrevTerm));
        }

        @Override
        public Optional<Term> lastTerm() {
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
        public ReplicatedLog commit(long committedIndex) {
            this.committedIndex = committedIndex;
            flush();
            return this;
        }

        @Override
        public ReplicatedLog append(LogEntry entry) {
            try {
                dirty = true;
                segmentOutput.writeStreamable(entry);
                this.entries.add(entry);
                return this;
            } catch (IOException e) {
                throw new IOError(e);
            }
        }

        @Override
        public ReplicatedLog append(ImmutableList<LogEntry> entries) {
            try {
                dirty = true;
                for (LogEntry entry : entries) {
                    segmentOutput.writeStreamable(entry);
                }
                this.entries.addAll(entries);
                return this;
            } catch (IOException e) {
                throw new IOError(e);
            }
        }

        @Override
        public ReplicatedLog append(ImmutableList<LogEntry> append, long prevIndex) {
            if (prevIndex == lastIndex()) {
                return append(append);
            } else {
                try {
                    for (int i = entries.size() - 1; i >= 0; i--) {
                        if (entries.get(i).getIndex() > prevIndex) {
                            entries.remove(i);
                        }
                    }
                    entries.addAll(append);

                    buffered.flush();
                    buffered.close();
                    segmentOutput.close();
                    File tmpSegment = temporaryFile();
                    try (StreamOutput output = streamService.output(tmpSegment, false)) {
                        for (LogEntry logEntry : entries) {
                            output.writeStreamable(logEntry);
                        }
                    }
                    Files.move(tmpSegment.toPath(), segmentFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

                    // recreate file object after move
                    File newSegmentFile = segmentFile(start);
                    BufferedOutputStream newBuffered = new BufferedOutputStream(new FileOutputStream(newSegmentFile, true));
                    StreamOutput newSegmentOutput = streamService.output(newBuffered);

                    this.segmentFile = newSegmentFile;
                    this.buffered = newBuffered;
                    this.segmentOutput = newSegmentOutput;
                    dirty = false;
                    return this;
                } catch (IOException e) {
                    throw new IOError(e);
                }
            }
        }

        @Override
        public ImmutableList<LogEntry> entriesBatchFrom(long fromIncluding, int howMany) {
            ImmutableList<LogEntry> toSend = slice(fromIncluding, fromIncluding + howMany);
            if (toSend.isEmpty()) {
                return toSend;
            } else {
                Term batchTerm = toSend.get(0).getTerm();
                ImmutableList.Builder<LogEntry> builder = ImmutableList.builder();
                for (LogEntry logEntry : toSend) {
                    if (logEntry.getTerm().equals(batchTerm)) {
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
        public Term termAt(long index) {
            if (index <= 0) {
                return new Term(0);
            } else if (!containsEntryAt(index)) {
                throw new IllegalArgumentException("Unable to find log entry at index " + index);
            } else {
                return get(index).getTerm();
            }
        }

        @Override
        public ReplicatedLog compactWith(RaftSnapshot snapshot, DiscoveryNode node) {
            long lastIncludedIndex = snapshot.getMeta().getLastIncludedIndex();
            LogEntry snapshotEntry = snapshot.toEntry(node);

            if (entries.isEmpty()) {
                entries.add(snapshotEntry);
            } else {
                if (entries.get(0).getIndex() <= lastIncludedIndex) {
                    entries.set(0, snapshotEntry);
                } else {
                    entries.add(0, snapshotEntry);
                }
                entries.removeIf(entry -> entry != snapshotEntry && entry.getIndex() <= lastIncludedIndex);
            }

            try {
                if (snapshot.getMeta().getLastIncludedIndex() == start) {
                    buffered.flush();
                    buffered.close();
                    segmentOutput.close();
                    File tmpSegment = temporaryFile();
                    try (StreamOutput output = streamService.output(tmpSegment, false)) {
                        for (LogEntry logEntry : entries) {
                            output.writeStreamable(logEntry);
                        }
                    }
                    Files.move(tmpSegment.toPath(), segmentFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

                    // recreate file object after move
                    File newSegmentFile = segmentFile(start);
                    BufferedOutputStream newBuffered = new BufferedOutputStream(new FileOutputStream(newSegmentFile, true));
                    StreamOutput newSegmentOutput = streamService.output(newBuffered);

                    this.segmentFile = newSegmentFile;
                    this.buffered = newBuffered;
                    this.segmentOutput = newSegmentOutput;
                    this.start = lastIncludedIndex;
                    dirty = false;
                    return this;
                } else {
                    buffered.flush();
                    buffered.close();
                    segmentOutput.close();
                    File newSegmentFile = segmentFile(snapshot.getMeta().getLastIncludedIndex());
                    BufferedOutputStream newBuffered = new BufferedOutputStream(new FileOutputStream(newSegmentFile, false));
                    StreamOutput newSegmentOutput = streamService.output(newBuffered);
                    for (LogEntry logEntry : entries) {
                        newSegmentOutput.writeStreamable(logEntry);
                    }
                    newBuffered.flush();
                    updateSegment(snapshot.getMeta().getLastIncludedIndex());

                    Files.delete(segmentFile.toPath());

                    this.segmentFile = newSegmentFile;
                    this.buffered = newBuffered;
                    this.segmentOutput = newSegmentOutput;
                    this.start = lastIncludedIndex;
                    dirty = false;
                    return this;
                }
            } catch (IOException e) {
                throw new IOError(e);
            }
        }

        @Override
        public boolean hasSnapshot() {
            return !entries.isEmpty() && entries.get(0).getCommand() instanceof RaftSnapshot;
        }

        @Override
        public RaftSnapshot snapshot() {
            return (RaftSnapshot) entries.get(0).getCommand();
        }

        private void flush() {
            if (dirty) {
                try {
                    buffered.flush();
                    dirty = false;
                } catch (IOException e) {
                    throw new IOError(e);
                }
            }
        }

        @Override
        public void close() throws IOException {
            flush();
            buffered.flush();
            buffered.close();
            segmentOutput.close();

            segmentFile = null;
            buffered = null;
            segmentOutput = null;
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
