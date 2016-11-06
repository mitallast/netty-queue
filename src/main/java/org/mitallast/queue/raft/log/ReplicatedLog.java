
package org.mitallast.queue.raft.log;

import com.google.common.collect.ImmutableList;
import org.mitallast.queue.raft.Term;
import org.mitallast.queue.raft.protocol.LogEntry;
import org.mitallast.queue.raft.protocol.RaftSnapshot;

import java.util.Optional;

public interface ReplicatedLog {

    ImmutableList<LogEntry> entries();

    long committedIndex();

    boolean containsMatchingEntry(Term otherPrevTerm, long otherPrevIndex);

    Optional<Term> lastTerm();

    long lastIndex();

    long prevIndex();

    long nextIndex();

    @ChangeState
    ReplicatedLog commit(long committedIndex);

    @ChangeState
    ReplicatedLog append(LogEntry entry);

    @ChangeState
    ReplicatedLog append(ImmutableList<LogEntry> entries);

    @ChangeState
    ReplicatedLog append(ImmutableList<LogEntry> entries, long take);

    ImmutableList<LogEntry> entriesBatchFrom(long fromIncluding);

    ImmutableList<LogEntry> entriesBatchFrom(long fromIncluding, int howMany);

    ImmutableList<LogEntry> slice(long from, long until);

    boolean containsEntryAt(long index);

    Term termAt(long index);

    @ChangeState
    ReplicatedLog compactedWith(RaftSnapshot snapshot);

    boolean hasSnapshot();

    RaftSnapshot snapshot();
}
