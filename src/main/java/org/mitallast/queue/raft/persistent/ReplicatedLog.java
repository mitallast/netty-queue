
package org.mitallast.queue.raft.persistent;

import javaslang.collection.Vector;
import javaslang.control.Option;
import org.mitallast.queue.raft.protocol.LogEntry;
import org.mitallast.queue.raft.protocol.RaftSnapshot;

import java.io.Closeable;

public interface ReplicatedLog extends Closeable {

    boolean isEmpty();

    boolean contains(LogEntry entry);

    Vector<LogEntry> entries();

    int committedEntries();

    long committedIndex();

    boolean containsMatchingEntry(long otherPrevTerm, long otherPrevIndex);

    Option<Long> lastTerm();

    long lastIndex();

    long prevIndex();

    long nextIndex();

    ReplicatedLog commit(long committedIndex);

    ReplicatedLog append(LogEntry entry);

    ReplicatedLog append(Vector<LogEntry> entries);

    ReplicatedLog compactWith(RaftSnapshot snapshot);

    Vector<LogEntry> entriesBatchFrom(long fromIncluding, int howMany);

    Vector<LogEntry> slice(long from, long until);

    boolean containsEntryAt(long index);

    long termAt(long index);

    boolean hasSnapshot();

    RaftSnapshot snapshot();

    @Override
    void close();
}
