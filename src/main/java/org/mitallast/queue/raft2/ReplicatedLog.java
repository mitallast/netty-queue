package org.mitallast.queue.raft2;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.UnmodifiableIterator;
import org.mitallast.queue.common.Immutable;
import org.mitallast.queue.log.Log;

import java.util.Optional;

public class ReplicatedLog {
    private final ImmutableList<LogEntry> entries;
    private final long commitedIndex;
    private final long start;

    public ReplicatedLog() {
        this(ImmutableList.of());
    }

    public ReplicatedLog(ImmutableList<LogEntry> entries) {
        this(entries, 0);
    }

    public ReplicatedLog(ImmutableList<LogEntry> entries, long commitedIndex) {
        this(entries, commitedIndex, 0);
    }

    public ReplicatedLog(ImmutableList<LogEntry> entries, long commitedIndex, long start) {
        this.entries = entries;
        this.commitedIndex = commitedIndex;
        this.start = start;
    }

    public ImmutableList<LogEntry> getEntries() {
        return entries;
    }

    public long getCommitedIndex() {
        return commitedIndex;
    }

    public long getStart() {
        return start;
    }

    public boolean containsMatchingEntry(Term otherPrevTerm, long otherPrevIndex) {
        return (otherPrevTerm.getTerm() == 0 && otherPrevIndex == 0) ||
                (containsEntryAt(otherPrevIndex) && termAt(otherPrevIndex).equals(otherPrevTerm) && lastIndex() == otherPrevIndex);
    }

    public Optional<Term> lastTerm() {
        if (entries.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(entries.get(entries.size() - 1).getTerm());
        }
    }

    public long lastIndex() {
        if (entries.isEmpty()) {
            return 0;
        } else {
            return entries.get(entries.size() - 1).getIndex();
        }
    }

    public long prevIndex() {
        return Math.max(lastIndex() - 1, 0);
    }

    public Term prevTerm() {
        if (entries.size() < 2) {
            return new Term(0);
        } else {
            return entries.get(1).getTerm();
        }
    }

    public long nextIndex() {
        if (entries.isEmpty()) {
            return 1;
        } else {
            return entries.get(entries.size() - 1).getIndex() + 1;
        }
    }

    public ReplicatedLog commit(int n) {
        return new ReplicatedLog(entries, n, start);
    }

    public ReplicatedLog append(LogEntry entry) {
        return append(entry, entries.size());
    }

    public ReplicatedLog append(LogEntry entry, long take) {
        return append(ImmutableList.of(entry), take);
    }

    public ReplicatedLog append(ImmutableList<LogEntry> entry, long take) {
        ImmutableList.Builder<LogEntry> builder = ImmutableList.builder();
        for (int i = 0; i < take; i++) {
            builder.add(entries.get(i));
        }
        builder.addAll(entry);
        return new ReplicatedLog(builder.build(), commitedIndex, start);
    }

    public ImmutableList<LogEntry> entriesBatchFrom(int fromIncluding) {
        return entriesBatchFrom(fromIncluding, 3);
    }

    public ImmutableList<LogEntry> entriesBatchFrom(int fromIncluding, int howMany) {
        ImmutableList<LogEntry> toSend = entries.subList(fromIncluding, fromIncluding + howMany);
        if (toSend.isEmpty()) {
            return toSend;
        } else {
            Term batchTerm = toSend.get(0).getTerm();
            ImmutableList.Builder<LogEntry> builder = ImmutableList.builder();
            for (LogEntry next : toSend) {
                if (next.getTerm().equals(batchTerm)) {
                    builder.add(next);
                } else {
                    break;
                }
            }
            return builder.build();
        }
    }

    public ImmutableList<LogEntry> between(int fromIndex, int toIndex) {
        return entries.subList(fromIndex, toIndex);
    }

    public boolean containsEntryAt(long index) {
        return entries.stream().anyMatch(entry -> entry.getIndex() == index);
    }

    public Term termAt(long index) {
        if (index <= 0) {
            return new Term(0);
        }
        Optional<LogEntry> logEntry = entries.stream().filter(entry -> entry.getIndex() == index).findFirst();
        if (logEntry.isPresent()) {
            return logEntry.get().getTerm();
        } else {
            throw new IllegalArgumentException("Unable to find log entry at index " + index);
        }
    }

    public ImmutableList<LogEntry> committedEntries() {
        return ImmutableList.<LogEntry>builder()
                .addAll(entries.stream().filter(entry -> entry.getIndex() <= commitedIndex).iterator())
                .build();
    }

    public ImmutableList<LogEntry> notCommittedEntries() {
        return ImmutableList.<LogEntry>builder()
                .addAll(entries.stream().filter(entry -> entry.getIndex() > commitedIndex).iterator())
                .build();
    }

    public ReplicatedLog compactedWith(RaftProtocol.RaftSnapshot snapshot) {
        boolean snapshotHasLaterTerm = snapshot.getMeta().getLastIncludedTerm().greater(lastTerm().orElse(new Term(0)));
        boolean snapshotHasLaterIndex = snapshot.getMeta().getLastIncludedIndex() > lastIndex();

        if (snapshotHasLaterTerm && snapshotHasLaterIndex) {
            return new ReplicatedLog(ImmutableList.of(snapshot.toEntry()));
        } else {
            ImmutableList.Builder<LogEntry> builder = ImmutableList.builder();
            builder.add(snapshot.toEntry());
            for (LogEntry entry : entries) {
                if (entry.getIndex() <= snapshot.getMeta().getLastIncludedIndex()) {
                    builder.add(entry);
                } else {
                    break;
                }
            }
            return new ReplicatedLog(builder.build(), commitedIndex, snapshot.getMeta().getLastIncludedIndex());
        }
    }

    public boolean hasShapshot() {
        return !entries.isEmpty() && entries.get(0).getCommand() instanceof RaftProtocol.RaftSnapshot;
    }

    public RaftProtocol.RaftSnapshot snapshot() {
        return (RaftProtocol.RaftSnapshot) entries.get(0).getCommand();
    }
}
