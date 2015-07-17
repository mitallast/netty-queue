package org.mitallast.queue.raft.action.append;

import com.google.common.collect.ImmutableList;
import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.validation.ValidationBuilder;
import org.mitallast.queue.raft.log.entry.RaftLogEntry;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;
import java.util.Collection;

public class AppendRequest implements ActionRequest<AppendRequest> {
    private final DiscoveryNode leader;
    private final long term;
    private final long logIndex;
    private final long logTerm;
    private final long commitIndex;
    private final long globalIndex;
    private final ImmutableList<RaftLogEntry> entries;

    private AppendRequest(DiscoveryNode leader, long term, long logIndex, long logTerm, long commitIndex, long globalIndex, ImmutableList<RaftLogEntry> entries) {
        this.leader = leader;
        this.term = term;
        this.logIndex = logIndex;
        this.logTerm = logTerm;
        this.commitIndex = commitIndex;
        this.globalIndex = globalIndex;
        this.entries = entries;
    }

    public static Builder builder() {
        return new Builder();
    }

    public DiscoveryNode leader() {
        return leader;
    }

    public long term() {
        return term;
    }

    public long logIndex() {
        return logIndex;
    }

    public long logTerm() {
        return logTerm;
    }

    public long commitIndex() {
        return commitIndex;
    }

    public long globalIndex() {
        return globalIndex;
    }

    public ImmutableList<RaftLogEntry> entries() {
        return entries;
    }

    @Override
    public ValidationBuilder validate() {
        return ValidationBuilder.builder();
    }

    @Override
    public String toString() {
        return "AppendRequest{" +
            "leader=" + leader +
            ", term=" + term +
            ", logIndex=" + logIndex +
            ", logTerm=" + logTerm +
            ", commitIndex=" + commitIndex +
            ", globalIndex=" + globalIndex +
                ", entries=" + entries.size() +
            '}';
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static class Builder implements EntryBuilder<AppendRequest> {
        private DiscoveryNode leader;
        private long term;
        private long logIndex;
        private long logTerm;
        private long commitIndex;
        private long globalIndex;
        private ImmutableList<RaftLogEntry> entries;

        private Builder from(AppendRequest entry) {
            leader = entry.leader;
            term = entry.term;
            logIndex = entry.logIndex;
            logTerm = entry.logTerm;
            commitIndex = entry.commitIndex;
            globalIndex = entry.globalIndex;
            entries = entry.entries;
            return this;
        }

        public Builder setLeader(DiscoveryNode leader) {
            this.leader = leader;
            return this;
        }

        public Builder setTerm(long term) {
            this.term = term;
            return this;
        }

        public Builder setLogIndex(long logIndex) {
            this.logIndex = logIndex;
            return this;
        }

        public Builder setLogTerm(long logTerm) {
            this.logTerm = logTerm;
            return this;
        }

        public Builder setCommitIndex(long commitIndex) {
            this.commitIndex = commitIndex;
            return this;
        }

        public Builder setGlobalIndex(long globalIndex) {
            this.globalIndex = globalIndex;
            return this;
        }

        public Builder setEntries(Collection<RaftLogEntry> entries) {
            this.entries = ImmutableList.<RaftLogEntry>builder().addAll(entries).build();
            return this;
        }

        public AppendRequest build() {
            return new AppendRequest(leader, term, logIndex, logTerm, commitIndex, globalIndex, entries);
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            leader = stream.readStreamable(DiscoveryNode::new);
            term = stream.readLong();
            logIndex = stream.readLong();
            logTerm = stream.readLong();
            commitIndex = stream.readLong();
            globalIndex = stream.readLong();
            int count = stream.readInt();
            ImmutableList.Builder<RaftLogEntry> builder = ImmutableList.builder();
            for (int i = 0; i < count; i++) {
                EntryBuilder<RaftLogEntry> entryBuilder = stream.readStreamable();
                RaftLogEntry entry = entryBuilder.build();
                builder.add(entry);
            }
            entries = builder.build();
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            stream.writeStreamable(leader);
            stream.writeLong(term);
            stream.writeLong(logIndex);
            stream.writeLong(logTerm);
            stream.writeLong(commitIndex);
            stream.writeLong(globalIndex);
            stream.writeInt(entries.size());
            for (RaftLogEntry entry : entries) {
                EntryBuilder entryBuilder = entry.toBuilder();
                stream.writeClass(entryBuilder.getClass());
                stream.writeStreamable(entryBuilder);
            }
        }
    }

}
