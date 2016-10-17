package org.mitallast.queue.raft2.protocol;

import com.google.common.collect.ImmutableList;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.raft2.RaftMessage;
import org.mitallast.queue.raft2.Term;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public class AppendEntries implements RaftMessage {
    private DiscoveryNode member;
    private Term term;
    private Term prevLogTerm;
    private long prevLogIndex;
    private long leaderCommit;
    private ImmutableList<LogEntry> entries;

    protected AppendEntries() {
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
    public void readFrom(StreamInput stream) throws IOException {
        member = stream.readStreamable(DiscoveryNode::new);
        term = new Term(stream.readLong());
        prevLogTerm = new Term(stream.readLong());
        prevLogIndex = stream.readLong();
        leaderCommit = stream.readLong();
        entries = stream.readStreamableList(LogEntry::new);
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
