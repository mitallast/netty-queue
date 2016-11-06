package org.mitallast.queue.raft.log;

import com.google.common.collect.ImmutableList;
import org.mitallast.queue.raft.Term;
import org.mitallast.queue.raft.protocol.LogEntry;
import org.mitallast.queue.raft.protocol.RaftSnapshot;

import java.util.Optional;

public class MemoryReplicatedLog implements ReplicatedLog {
    private final ImmutableList<LogEntry> entries;
    private final long committedIndex;
    private final long start;

    public MemoryReplicatedLog() {
        this(ImmutableList.of(), 0);
    }

    public MemoryReplicatedLog(ImmutableList<LogEntry> entries, long committedIndex) {
        this(entries, committedIndex, 1);
    }

    public MemoryReplicatedLog(ImmutableList<LogEntry> entries, long committedIndex, long start) {
        this.entries = entries;
        this.committedIndex = committedIndex;
        this.start = start;
    }

    @Override
    public ImmutableList<LogEntry> entries() {
        return entries;
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
        return (otherPrevTerm.getTerm() == 0 && otherPrevIndex == 0) ||
                (containsEntryAt(otherPrevIndex) && termAt(otherPrevIndex).equals(otherPrevTerm) && lastIndex() == otherPrevIndex);
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
        return new MemoryReplicatedLog(entries, committedIndex, start);
    }

    @Override
    public ReplicatedLog append(LogEntry entry) {
        return append(ImmutableList.of(entry));
    }

    @Override
    public ReplicatedLog append(ImmutableList<LogEntry> entries) {
        return new MemoryReplicatedLog(ImmutableList.<LogEntry>builder()
                .addAll(this.entries)
                .addAll(entries).build(), committedIndex, start);
    }

    @Override
    public ReplicatedLog append(ImmutableList<LogEntry> entries, long take) {
        return new MemoryReplicatedLog(ImmutableList.<LogEntry>builder().addAll(take(take)).addAll(entries).build(), committedIndex, start);
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

        if (snapshotHasLaterTerm && snapshotHasLaterIndex) {
            return new MemoryReplicatedLog(ImmutableList.of(snapshot.toEntry()), committedIndex, snapshot.getMeta().getLastIncludedIndex());
        } else {
            return new MemoryReplicatedLog(ImmutableList.<LogEntry>builder()
                    .add(snapshot.toEntry())
                    .addAll(entries.stream().filter(entry -> entry.getIndex() > snapshot.getMeta().getLastIncludedIndex()).iterator())
                    .build(), committedIndex, snapshot.getMeta().getLastIncludedIndex());
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
        return "MemoryReplicatedLog{" +
                "entries=" + entries +
                ", committedIndex=" + committedIndex +
                ", start=" + start +
                '}';
    }
}
