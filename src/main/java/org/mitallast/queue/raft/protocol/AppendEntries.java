package org.mitallast.queue.raft.protocol;

import javaslang.collection.Vector;
import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;
import org.mitallast.queue.transport.DiscoveryNode;

public class AppendEntries implements Message {
    public static final Codec<AppendEntries> codec = Codec.Companion.of(
        AppendEntries::new,
        AppendEntries::getMember,
        AppendEntries::getTerm,
        AppendEntries::getPrevLogTerm,
        AppendEntries::getPrevLogIndex,
        AppendEntries::getLeaderCommit,
        AppendEntries::getEntries,
        DiscoveryNode.codec,
        Codec.Companion.longCodec(),
        Codec.Companion.longCodec(),
        Codec.Companion.longCodec(),
        Codec.Companion.longCodec(),
        Codec.Companion.vectorCodec(LogEntry.codec)
    );

    private final DiscoveryNode member;
    private final long term;
    private final long prevLogTerm;
    private final long prevLogIndex;
    private final long leaderCommit;
    private final Vector<LogEntry> entries;

    public AppendEntries(DiscoveryNode member, long term, long prevLogTerm, long prevLogIndex, long leaderCommit, Vector<LogEntry> entries) {
        this.member = member;
        this.term = term;
        this.prevLogTerm = prevLogTerm;
        this.prevLogIndex = prevLogIndex;
        this.leaderCommit = leaderCommit;
        this.entries = entries;
    }

    public DiscoveryNode getMember() {
        return member;
    }

    public long getTerm() {
        return term;
    }

    public long getPrevLogTerm() {
        return prevLogTerm;
    }

    public long getPrevLogIndex() {
        return prevLogIndex;
    }

    public long getLeaderCommit() {
        return leaderCommit;
    }

    public Vector<LogEntry> getEntries() {
        return entries;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AppendEntries that = (AppendEntries) o;

        if (prevLogIndex != that.prevLogIndex) return false;
        if (leaderCommit != that.leaderCommit) return false;
        if (!member.equals(that.member)) return false;
        if (term != that.term) return false;
        if (prevLogTerm != that.prevLogTerm) return false;
        return entries.equals(that.entries);

    }

    @Override
    public int hashCode() {
        int result = member.hashCode();
        result = 31 * result + (int) (term ^ (term >>> 32));
        result = 31 * result + (int) (prevLogTerm ^ (prevLogTerm >>> 32));
        result = 31 * result + (int) (prevLogIndex ^ (prevLogIndex >>> 32));
        result = 31 * result + (int) (leaderCommit ^ (leaderCommit >>> 32));
        result = 31 * result + entries.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "AppendEntries{" +
            "member=" + member +
            ", term=" + term +
            ", prevLogTerm=" + prevLogTerm +
            ", prevLogIndex=" + prevLogIndex +
            ", leaderCommit=" + leaderCommit +
            ", entries=" + entries +
            '}';
    }
}
