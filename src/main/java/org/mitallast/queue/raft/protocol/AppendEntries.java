package org.mitallast.queue.raft.protocol;

import com.google.common.collect.ImmutableList;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.raft.Term;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public class AppendEntries implements Streamable {
    private final DiscoveryNode member;
    private final Term term;
    private final Term prevLogTerm;
    private final long prevLogIndex;
    private final long leaderCommit;
    private final ImmutableList<LogEntry> entries;

    public AppendEntries(StreamInput stream) throws IOException {
        member = stream.readStreamable(DiscoveryNode::new);
        term = new Term(stream.readLong());
        prevLogTerm = new Term(stream.readLong());
        prevLogIndex = stream.readLong();
        leaderCommit = stream.readLong();
        entries = stream.readStreamableList(LogEntry::new);
    }

    public AppendEntries(DiscoveryNode member, Term term, Term prevLogTerm, long prevLogIndex, ImmutableList<LogEntry> entries, long leaderCommit) {
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

    public Term getTerm() {
        return term;
    }

    public Term getPrevLogTerm() {
        return prevLogTerm;
    }

    public long getPrevLogIndex() {
        return prevLogIndex;
    }

    public long getLeaderCommit() {
        return leaderCommit;
    }

    public ImmutableList<LogEntry> getEntries() {
        return entries;
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeStreamable(member);
        stream.writeLong(term.getTerm());
        stream.writeLong(prevLogTerm.getTerm());
        stream.writeLong(prevLogIndex);
        stream.writeLong(leaderCommit);
        stream.writeStreamableList(entries);
    }
}
