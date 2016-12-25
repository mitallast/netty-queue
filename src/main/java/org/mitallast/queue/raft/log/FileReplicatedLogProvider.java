package org.mitallast.queue.raft.log;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Provider;
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

public class FileReplicatedLogProvider extends AbstractComponent implements Provider<ReplicatedLog> {

    private final static long initialIndex = 1;
    private final static long initialCommittedIndex = 0;

    private final FileService fileService;
    private final StreamService streamService;

    private final RandomAccessFile stateFile;

    @Inject
    public FileReplicatedLogProvider(Config config, FileService fileService, StreamService streamService) throws IOException {
        super(config.getConfig("raft"), FileReplicatedLogProvider.class);
        this.fileService = fileService;
        this.streamService = streamService;
        File stateFile = fileService.resource("raft", "state.bin");
        this.stateFile = new RandomAccessFile(stateFile, "rw");
    }

    @Override
    public ReplicatedLog get() {
        try {
            final long index;

            if (stateFile.length() == 0) {
                index = initialIndex;
            } else {
                stateFile.seek(0);
                index = stateFile.readLong();
            }

            final File segmentFile = segmentFile(index);
            ArrayList<LogEntry> entries = new ArrayList<>();
            try (StreamInput input = streamService.input(segmentFile)) {
                while (input.available() > 0) {
                    entries.add(input.readStreamable(LogEntry::new));
                }
            }
            BufferedOutputStream segmentOutput_ = new BufferedOutputStream(new FileOutputStream(segmentFile, true), 65536);
            StreamOutput segmentOutput = streamService.output(segmentOutput_);
            return new FileReplicatedLog(
                segmentFile,
                segmentOutput_,
                segmentOutput,
                entries,
                initialCommittedIndex,
                index
            );
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    private File segmentFile(long input) throws IOException {
        return fileService.resource("raft", input + ".log");
    }

    private File temporaryFile() throws IOException {
        return fileService.temporary("raft", "log.", ".tmp");
    }

    private void writeState(long index) throws IOException {
        stateFile.seek(0);
        stateFile.writeLong(index);
    }

    public class FileReplicatedLog implements ReplicatedLog {
        private final ArrayList<LogEntry> entries;
        private boolean dirty = false;
        private long start;

        private File segmentFile;
        private OutputStream segmentOutput_;
        private StreamOutput segmentOutput;

        private long committedIndex;

        public FileReplicatedLog(File segmentFile, OutputStream segmentOutput_, StreamOutput segmentOutput, ArrayList<LogEntry> entries, long committedIndex, long start) throws IOException {
            this.entries = entries;
            this.committedIndex = committedIndex;
            this.start = start;
            this.segmentFile = segmentFile;
            this.segmentOutput_ = segmentOutput_;
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

                    segmentOutput_.flush();
                    segmentOutput_.close();
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
                    BufferedOutputStream newSegmentOutput_ = new BufferedOutputStream(new FileOutputStream(newSegmentFile, true));
                    StreamOutput newSegmentOutput = streamService.output(newSegmentOutput_);

                    this.segmentFile = newSegmentFile;
                    this.segmentOutput_ = newSegmentOutput_;
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
                    segmentOutput_.flush();
                    segmentOutput_.close();
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
                    BufferedOutputStream newSegmentOutput_ = new BufferedOutputStream(new FileOutputStream(newSegmentFile, true));
                    StreamOutput newSegmentOutput = streamService.output(newSegmentOutput_);

                    this.segmentFile = newSegmentFile;
                    this.segmentOutput_ = newSegmentOutput_;
                    this.segmentOutput = newSegmentOutput;
                    this.start = lastIncludedIndex;
                    dirty = false;
                    return this;
                } else {
                    segmentOutput_.flush();
                    segmentOutput_.close();
                    segmentOutput.close();
                    File newSegmentFile = segmentFile(snapshot.getMeta().getLastIncludedIndex());
                    BufferedOutputStream newSegmentOutput_ = new BufferedOutputStream(new FileOutputStream(newSegmentFile, false));
                    StreamOutput newSegmentOutput = streamService.output(newSegmentOutput_);
                    for (LogEntry logEntry : entries) {
                        newSegmentOutput.writeStreamable(logEntry);
                    }
                    newSegmentOutput_.flush();
                    writeState(snapshot.getMeta().getLastIncludedIndex());

                    Files.delete(segmentFile.toPath());

                    this.segmentFile = newSegmentFile;
                    this.segmentOutput_ = newSegmentOutput_;
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
            if(dirty) {
                try {
                    segmentOutput_.flush();
                    dirty = false;
                } catch (IOException e) {
                    throw new IOError(e);
                }
            }
        }

        @Override
        public String toString() {
            return "FileReplicatedLog{" +
                "entries=" + entries +
                ", committedIndex=" + committedIndex +
                ", start=" + start +
                ", file=" + segmentFile + " (" + segmentFile.length() + " bytes)" +
                '}';
        }
    }
}
