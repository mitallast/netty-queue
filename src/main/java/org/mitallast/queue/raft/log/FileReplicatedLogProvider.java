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

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Optional;

public class FileReplicatedLogProvider extends AbstractComponent implements Provider<ReplicatedLog> {

    private final static long initialIndex = 1;
    private final static long initialCommittedIndex = 0;

    private final FileService fileService;
    private final StreamService streamService;

    private final File stateFile;

    @Inject
    public FileReplicatedLogProvider(Config config, FileService fileService, StreamService streamService) throws IOException {
        super(config.getConfig("raft"), FileReplicatedLogProvider.class);
        this.fileService = fileService;
        this.streamService = streamService;
        this.stateFile = fileService.resource("raft", "state.bin");
    }

    @Override
    public ReplicatedLog get() {
        try {
            final long index;
            final long committedIndex;

            if (stateFile.length() == 0) {
                index = initialIndex;
                committedIndex = initialCommittedIndex;
            } else {
                try (StreamInput input = streamService.input(stateFile)) {
                    index = input.readLong();
                    committedIndex = input.readLong();
                }
            }

            final File segmentFile = segmentFile(index);
            ImmutableList.Builder<LogEntry> builder = ImmutableList.builder();
            try (StreamInput input = streamService.input(segmentFile)) {
                while (input.available() > 0) {
                    builder.add(input.readStreamable(LogEntry::new));
                }
            }
            return new FileReplicatedLog(segmentFile, streamService.output(segmentFile, true), builder.build(), committedIndex, index);
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

    private int compareSegments(Path a, Path b) {
        return Long.compare(segmentIndex(a), segmentIndex(b));
    }

    private long segmentIndex(Path path) {
        String name = path.getFileName().toString();
        return Long.parseLong(name.substring(0, name.length() - 4));
    }

    private void writeState(long index, long committedIndex) throws IOException {
        try (StreamOutput output = streamService.output(stateFile)) {
            output.writeLong(index);
            output.writeLong(committedIndex);
        }
    }

    public class FileReplicatedLog implements ReplicatedLog {
        private final ImmutableList<LogEntry> entries;
        private final long committedIndex;
        private final long start;

        private final File segmentFile;
        private final StreamOutput segmentOutput;

        public FileReplicatedLog(File segmentFile, StreamOutput segmentOutput, ImmutableList<LogEntry> entries, long committedIndex, long start) throws IOException {
            this.entries = entries;
            this.committedIndex = committedIndex;
            this.start = start;
            this.segmentFile = segmentFile;
            this.segmentOutput = segmentOutput;
        }

        @Override
        public ImmutableList<LogEntry> entries() {
            return entries;
        }

        @Override
        public int committedEntries() {
            return (int) (committedIndex - start);
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

        private ImmutableList<LogEntry> take(long n) {
            return entries.subList(0, (int) (n - offset()));
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
                    (!entries().isEmpty() && otherPrevIndex >= committedIndex() && containsEntryAt(otherPrevIndex) && termAt(otherPrevIndex).equals(otherPrevTerm));
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
            try {
                writeState(start, committedIndex);
                return new FileReplicatedLog(segmentFile, segmentOutput, entries, committedIndex, start);
            } catch (IOException e) {
                throw new IOError(e);
            }
        }

        @Override
        public ReplicatedLog append(LogEntry entry) {
            return append(ImmutableList.of(entry));
        }

        @Override
        public ReplicatedLog append(ImmutableList<LogEntry> entries) {
            try {
                for (LogEntry entry : entries) {
                    segmentOutput.writeStreamable(entry);
                }
                return new FileReplicatedLog(segmentFile, segmentOutput, ImmutableList.<LogEntry>builder()
                        .addAll(this.entries)
                        .addAll(entries).build(), committedIndex, start);
            } catch (IOException e) {
                throw new IOError(e);
            }
        }

        @Override
        public ReplicatedLog append(ImmutableList<LogEntry> entries, long take) {
            if (take == entries.size() - offset()) {
                return append(entries);
            } else {
                try {
                    ImmutableList<LogEntry> logEntries = ImmutableList.<LogEntry>builder().addAll(take(take)).addAll(entries).build();
                    segmentOutput.close();
                    File tmpSegment = temporaryFile();
                    try (StreamOutput output = streamService.output(tmpSegment, false)) {
                        for (LogEntry logEntry : logEntries) {
                            output.writeStreamable(logEntry);
                        }
                    }
                    Files.move(tmpSegment.toPath(), segmentFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

                    // recreate file object after move
                    File newSegmentFile = segmentFile(start);
                    StreamOutput newSegmentOutput = streamService.output(segmentFile, true);
                    return new FileReplicatedLog(newSegmentFile, newSegmentOutput, logEntries, committedIndex, start);
                } catch (IOException e) {
                    throw new IOError(e);
                }
            }
        }

        @Override
        public ImmutableList<LogEntry> entriesBatchFrom(long fromIncluding) {
            return entriesBatchFrom(fromIncluding, 3);
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
            return entries.subList(Math.max(0, fromIndex), Math.min(toIndex, entries.size()));
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
        public ReplicatedLog compactedWith(RaftSnapshot snapshot) {
            boolean snapshotHasLaterTerm = snapshot.getMeta().getLastIncludedTerm().greater(lastTerm().orElse(new Term(0)));
            boolean snapshotHasLaterIndex = snapshot.getMeta().getLastIncludedIndex() > lastIndex();

            final ImmutableList<LogEntry> logEntries;

            if (snapshotHasLaterTerm && snapshotHasLaterIndex) {
                logEntries = ImmutableList.of(snapshot.toEntry());
            } else {
                logEntries = ImmutableList.<LogEntry>builder()
                        .add(snapshot.toEntry())
                        .addAll(entries.stream().filter(entry -> entry.getIndex() > snapshot.getMeta().getLastIncludedIndex()).iterator())
                        .build();
            }

            try {

                if (snapshot.getMeta().getLastIncludedIndex() == start) {
                    segmentOutput.close();
                    File tmpSegment = temporaryFile();
                    try (StreamOutput output = streamService.output(tmpSegment, false)) {
                        for (LogEntry logEntry : logEntries) {
                            output.writeStreamable(logEntry);
                        }
                    }
                    Files.move(tmpSegment.toPath(), segmentFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

                    // recreate file object after move
                    File newSegmentFile = segmentFile(start);
                    StreamOutput newSegmentOutput = streamService.output(segmentFile, true);
                    return new FileReplicatedLog(newSegmentFile, newSegmentOutput, logEntries, committedIndex, start);
                } else {
                    segmentOutput.close();
                    File newSegmentFile = segmentFile(snapshot.getMeta().getLastIncludedIndex());
                    StreamOutput newSegmentOutput = streamService.output(newSegmentFile, false);
                    for (LogEntry logEntry : logEntries) {
                        newSegmentOutput.writeStreamable(logEntry);
                    }
                    writeState(snapshot.getMeta().getLastIncludedIndex(), committedIndex);
                    segmentFile.delete();
                    return new FileReplicatedLog(newSegmentFile, newSegmentOutput, logEntries, committedIndex, snapshot.getMeta().getLastIncludedIndex());
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

        @Override
        public String toString() {
            return "FileReplicatedLog{" +
                    "entries=" + entries +
                    ", committedIndex=" + committedIndex +
                    ", start=" + start +
                    '}';
        }
    }
}
