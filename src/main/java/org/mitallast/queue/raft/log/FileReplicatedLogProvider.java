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
            ImmutableList.Builder<LogEntry> builder = ImmutableList.builder();
            try (StreamInput input = streamService.input(segmentFile)) {
                while (input.available() > 0) {
                    builder.add(input.readStreamable(LogEntry::new));
                }
            }
            BufferedOutputStream segmentOutput_ = new BufferedOutputStream(new FileOutputStream(segmentFile, true));
            StreamOutput segmentOutput = streamService.output(segmentOutput_);
            return new FileReplicatedLog(
                segmentFile,
                segmentOutput_,
                segmentOutput,
                builder.build(),
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
        private final ImmutableList<LogEntry> entries;
        private final long committedIndex;
        private final long start;

        private final File segmentFile;
        private final OutputStream segmentOutput_;
        private final StreamOutput segmentOutput;

        public FileReplicatedLog(File segmentFile, OutputStream segmentOutput_, StreamOutput segmentOutput, ImmutableList<LogEntry> entries, long committedIndex, long start) throws IOException {
            this.entries = entries;
            this.committedIndex = committedIndex;
            this.start = start;
            this.segmentFile = segmentFile;
            this.segmentOutput_ = segmentOutput_;
            this.segmentOutput = segmentOutput;
        }

        @Override
        public ImmutableList<LogEntry> entries() {
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
                return new FileReplicatedLog(segmentFile, segmentOutput_, segmentOutput, entries, committedIndex, start);
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
                    segmentOutput_.flush();
                }
                return new FileReplicatedLog(segmentFile, segmentOutput_, segmentOutput, ImmutableList.<LogEntry>builder()
                    .addAll(this.entries)
                    .addAll(entries).build(), committedIndex, start);
            } catch (IOException e) {
                throw new IOError(e);
            }
        }

        @Override
        public ReplicatedLog append(ImmutableList<LogEntry> entries, long prevIndex) {
            if (prevIndex == lastIndex()) {
                return append(entries);
            } else {
                try {
                    ImmutableList<LogEntry> logEntries = ImmutableList.<LogEntry>builder()
                        .addAll(slice(0, prevIndex))
                        .addAll(entries).build();
                    segmentOutput_.flush();
                    segmentOutput_.close();
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
                    BufferedOutputStream newSegmentOutput_ = new BufferedOutputStream(new FileOutputStream(newSegmentFile, true));
                    StreamOutput newSegmentOutput = streamService.output(newSegmentOutput_);
                    return new FileReplicatedLog(newSegmentFile, newSegmentOutput_, newSegmentOutput, logEntries, committedIndex, start);
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
        public ReplicatedLog compactedWith(RaftSnapshot snapshot, DiscoveryNode node) {
            boolean snapshotHasLaterTerm = snapshot.getMeta().getLastIncludedTerm().greater(lastTerm().orElse(new Term(0)));
            boolean snapshotHasLaterIndex = snapshot.getMeta().getLastIncludedIndex() > lastIndex();

            final ImmutableList<LogEntry> logEntries;

            if (snapshotHasLaterTerm && snapshotHasLaterIndex) {
                logEntries = ImmutableList.of(snapshot.toEntry(node));
            } else {
                logEntries = ImmutableList.<LogEntry>builder()
                    .add(snapshot.toEntry(node))
                    .addAll(entries.stream().filter(entry -> entry.getIndex() > snapshot.getMeta().getLastIncludedIndex()).iterator())
                    .build();
            }

            try {

                if (snapshot.getMeta().getLastIncludedIndex() == start) {
                    segmentOutput_.flush();
                    segmentOutput_.close();
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
                    BufferedOutputStream newSegmentOutput_ = new BufferedOutputStream(new FileOutputStream(newSegmentFile, true));
                    StreamOutput newSegmentOutput = streamService.output(newSegmentOutput_);
                    return new FileReplicatedLog(newSegmentFile, newSegmentOutput_, newSegmentOutput, logEntries, committedIndex, start);
                } else {
                    segmentOutput_.flush();
                    segmentOutput_.close();
                    segmentOutput.close();
                    File newSegmentFile = segmentFile(snapshot.getMeta().getLastIncludedIndex());
                    BufferedOutputStream newSegmentOutput_ = new BufferedOutputStream(new FileOutputStream(newSegmentFile, false));
                    StreamOutput newSegmentOutput = streamService.output(newSegmentOutput_);
                    for (LogEntry logEntry : logEntries) {
                        newSegmentOutput.writeStreamable(logEntry);
                    }
                    newSegmentOutput_.flush();
                    writeState(snapshot.getMeta().getLastIncludedIndex());
                    segmentFile.delete();
                    return new FileReplicatedLog(newSegmentFile, newSegmentOutput_, newSegmentOutput, logEntries, committedIndex, snapshot.getMeta().getLastIncludedIndex());
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
                ", file=" + segmentFile + " (" + segmentFile.length() + " bytes)" +
                '}';
        }
    }
}
