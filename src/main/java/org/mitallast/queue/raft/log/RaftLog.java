package org.mitallast.queue.raft.log;

import org.mitallast.queue.raft.log.entry.RaftLogEntry;

import java.io.IOException;

public interface RaftLog {

    long firstIndex();

    long nextIndex();

    long lastIndex();

    long appendEntry(RaftLogEntry entry) throws IOException;

    <T extends RaftLogEntry> T getEntry(long index) throws IOException;

    boolean containsIndex(long index);

    boolean containsEntry(long index) throws IOException;

    void skip(long entries) throws IOException;

    void truncate(long index) throws IOException;

    void delete() throws IOException;
}
