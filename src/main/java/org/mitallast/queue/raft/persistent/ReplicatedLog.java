
package org.mitallast.queue.raft.persistent;

import com.google.common.collect.ImmutableList;
import org.mitallast.queue.proto.raft.DiscoveryNode;
import org.mitallast.queue.proto.raft.LogEntry;
import org.mitallast.queue.proto.raft.RaftSnapshot;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

public interface ReplicatedLog extends Closeable {

    boolean isEmpty();

    boolean contains(LogEntry entry);

    ImmutableList<LogEntry> entries();

    int committedEntries();

    long committedIndex();

    boolean containsMatchingEntry(long otherPrevTerm, long otherPrevIndex);

    Optional<Long> lastTerm();

    long lastIndex();

    long prevIndex();

    long nextIndex();

    ReplicatedLog commit(long committedIndex) throws IOException;

    ReplicatedLog append(LogEntry entry) throws IOException;

    ReplicatedLog append(List<LogEntry> entries) throws IOException;

    ReplicatedLog append(List<LogEntry> entries, long prevIndex) throws IOException;

    ReplicatedLog compactWith(RaftSnapshot snapshot, DiscoveryNode node) throws IOException;

    ImmutableList<LogEntry> entriesBatchFrom(long fromIncluding, int howMany);

    ImmutableList<LogEntry> slice(long from, long until);

    boolean containsEntryAt(long index);

    long termAt(long index);

    boolean hasSnapshot();

    RaftSnapshot snapshot();

    @Override
    void close() throws IOException;
}
