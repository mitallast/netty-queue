package org.mitallast.queue.raft.log;

import com.google.common.collect.ImmutableList;
import org.mitallast.queue.common.Immutable;
import org.mitallast.queue.raft.Term;
import org.mitallast.queue.raft.protocol.LogEntry;
import org.mitallast.queue.raft.protocol.RaftSnapshot;

import java.util.Optional;
import java.util.function.Predicate;

public class ReplicatedLog {
    private final ImmutableList<LogEntry> entries;
    private final long committedIndex;
    private final long start;

    public ReplicatedLog() {
        this(ImmutableList.of(), 0);
    }

    public ReplicatedLog(ImmutableList<LogEntry> entries, long committedIndex) {
        this(entries, committedIndex, 1);
    }

    public ReplicatedLog(ImmutableList<LogEntry> entries, long committedIndex, long start) {
        this.entries = entries;
        this.committedIndex = committedIndex;
        this.start = start;
    }

    public ImmutableList<LogEntry> entries() {
        return entries;
    }

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

    private boolean exists(Predicate<LogEntry> predicate) {
        return entries.stream().anyMatch(predicate);
    }

    private Optional<LogEntry> find(Predicate<LogEntry> predicate) {
        return entries.stream().filter(predicate).findFirst();
    }

    private ImmutableList<LogEntry> filter(Predicate<LogEntry> predicate) {
        return ImmutableList.copyOf(entries.stream().filter(predicate).iterator());
    }

    private LogEntry get(long index) {
        return entries.get((int) (index - start));
    }

    private LogEntry last() {
        return entries.get(entries.size() - 1);
    }

    public boolean containsMatchingEntry(Term otherPrevTerm, long otherPrevIndex) {
        return (otherPrevTerm.getTerm() == 0 && otherPrevIndex == 0) ||
                (containsEntryAt(otherPrevIndex) && termAt(otherPrevIndex).equals(otherPrevTerm) && lastIndex() == otherPrevIndex);
    }

    public Optional<Term> lastTerm() {
        return (entries.isEmpty()) ? Optional.empty() : Optional.of(last().getTerm());
    }

    public long lastIndex() {
        return (entries.isEmpty()) ? 0 : last().getIndex();
    }

    public long prevIndex() {
        return Math.max(0, lastIndex() - 1);
    }

    public Term prevTerm() {
        return (entries.size() < 2) ? new Term(0) : entries.get(entries.size() - 1).getTerm();
    }

    public long nextIndex() {
        return entries.isEmpty() ? 1 : last().getIndex() + 1;
    }

    public ReplicatedLog commit(long committedIndex) {
        return new ReplicatedLog(entries, committedIndex, start);
    }

    public ReplicatedLog append(LogEntry entry) {
        return append(ImmutableList.of(entry));
    }

    public ReplicatedLog append(ImmutableList<LogEntry> entries) {
        return new ReplicatedLog(ImmutableList.<LogEntry>builder()
                .addAll(this.entries)
                .addAll(entries).build(), committedIndex, start);
    }

    public ReplicatedLog append(ImmutableList<LogEntry> entries, long take) {
        return new ReplicatedLog(ImmutableList.<LogEntry>builder().addAll(take(take)).addAll(entries).build(), committedIndex, start);
    }

    public ImmutableList<LogEntry> entriesBatchFrom(long fromIncluding) {
        return entriesBatchFrom(fromIncluding, 3);
    }

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

    public ImmutableList<LogEntry> slice(long from, long until) {
        int fromIndex = (int) (from - offset());
        int toIndex = (int) (until - offset());
        if (fromIndex >= entries.size()) {
            return ImmutableList.of();
        }
        return entries.subList(fromIndex, Math.min(toIndex, entries.size()));
    }

    public boolean containsEntryAt(long index) {
        return index >= start && index <= length() && get(index).getIndex() == index;
    }

    public Term termAt(long index) {
        if (index <= 0) {
            return new Term(0);
        } else if (!containsEntryAt(index)) {
            throw new IllegalArgumentException("Unable to find log entry at index " + index);
        } else {
            return get(index).getTerm();
        }
    }

    public ImmutableList<LogEntry> committedEntries() {
        return take(committedIndex);
    }

    public ImmutableList<LogEntry> notCommittedEntries() {
        return slice(committedIndex + 1, length());
    }

    public ReplicatedLog compactedWith(RaftSnapshot snapshot) {
        boolean snapshotHasLaterTerm = snapshot.getMeta().getLastIncludedTerm().greater(lastTerm().orElse(new Term(0)));
        boolean snapshotHasLaterIndex = snapshot.getMeta().getLastIncludedIndex() > lastIndex();

        if (snapshotHasLaterTerm && snapshotHasLaterIndex) {
            return new ReplicatedLog(ImmutableList.of(snapshot.toEntry()), committedIndex, snapshot.getMeta().getLastIncludedIndex());
        } else {
            return new ReplicatedLog(ImmutableList.<LogEntry>builder()
                    .add(snapshot.toEntry())
                    .addAll(entries.stream().filter(entry -> entry.getIndex() > snapshot.getMeta().getLastIncludedIndex()).iterator())
                    .build(), committedIndex, snapshot.getMeta().getLastIncludedIndex());
        }
    }

    public boolean hasSnapshot() {
        return !entries.isEmpty() && entries.get(0).getCommand() instanceof RaftSnapshot;
    }

    public RaftSnapshot snapshot() {
        return (RaftSnapshot) entries.get(0).getCommand();
    }
}
