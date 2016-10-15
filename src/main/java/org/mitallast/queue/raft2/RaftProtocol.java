package org.mitallast.queue.raft2;

import com.google.common.collect.ImmutableList;
import org.mitallast.queue.raft.state.RaftState;
import org.mitallast.queue.transport.DiscoveryNode;

import java.util.Optional;

public interface RaftProtocol {
    class ClientMessage {
        private final DiscoveryNode client;
        private final Object cmd;

        public ClientMessage(DiscoveryNode client, Object cmd) {
            this.client = client;
            this.cmd = cmd;
        }

        public DiscoveryNode getClient() {
            return client;
        }

        public Object getCmd() {
            return cmd;
        }
    }

    interface ElectionMessage {
    }

    class BeginElection implements ElectionMessage {
    }

    class ElectedAsLeader implements ElectionMessage {
    }

    class ElectionTimeout implements ElectionMessage {
    }

    class RequestVote {
        private final Term term;
        private final DiscoveryNode candidate;
        private final Term lastLogTerm;
        private final long lastLogIndex;

        public RequestVote(Term term, DiscoveryNode candidate, Term lastLogTerm, long lastLogIndex) {
            this.term = term;
            this.candidate = candidate;
            this.lastLogTerm = lastLogTerm;
            this.lastLogIndex = lastLogIndex;
        }

        public Term getTerm() {
            return term;
        }

        public DiscoveryNode getCandidate() {
            return candidate;
        }

        public Term getLastLogTerm() {
            return lastLogTerm;
        }

        public long getLastLogIndex() {
            return lastLogIndex;
        }
    }

    class VoteCandidate {
        private final Term term;

        public VoteCandidate(Term term) {
            this.term = term;
        }

        public Term getTerm() {
            return term;
        }
    }

    class DeclineCandidate {
        private final Term term;

        public DeclineCandidate(Term term) {
            this.term = term;
        }

        public Term getTerm() {
            return term;
        }
    }

    class AppendEntries {
        private final Term term;
        private final Term prevLogTerm;
        private final long prevLogIndex;
        private final ImmutableList<LogEntry> entries;
        private final long leaderCommit;

        public AppendEntries(Term term, Term prevLogTerm, long prevLogIndex, ImmutableList<LogEntry> entries, long leaderCommit) {
            this.term = term;
            this.prevLogTerm = prevLogTerm;
            this.prevLogIndex = prevLogIndex;
            this.entries = entries;
            this.leaderCommit = leaderCommit;
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

        public ImmutableList<LogEntry> getEntries() {
            return entries;
        }

        public long getLeaderCommit() {
            return leaderCommit;
        }
    }

    class AppendRejected {
        private final Term term;

        public AppendRejected(Term term) {
            this.term = term;
        }

        public Term getTerm() {
            return term;
        }
    }

    class AppendSuccessful {
        private final Term term;
        private final long lastIndex;

        public AppendSuccessful(Term term, long lastIndex) {
            this.term = term;
            this.lastIndex = lastIndex;
        }

        public Term getTerm() {
            return term;
        }

        public long getLastIndex() {
            return lastIndex;
        }
    }

    class InitLogSnapshot {
    }

    class InstallSnapshot {
        private final Term term;
        private final RaftSnapshot snapshot;

        public InstallSnapshot(Term term, RaftSnapshot snapshot) {
            this.term = term;
            this.snapshot = snapshot;
        }

        public Term getTerm() {
            return term;
        }

        public RaftSnapshot getSnapshot() {
            return snapshot;
        }
    }

    class InstallSnapshotRejected {
        private final Term term;

        public InstallSnapshotRejected(Term term) {
            this.term = term;
        }

        public Term getTerm() {
            return term;
        }
    }

    class InstallSnapshotSuccessful {
        private final Term term;
        private final long lastIndex;

        public InstallSnapshotSuccessful(Term term, long lastIndex) {
            this.term = term;
            this.lastIndex = lastIndex;
        }

        public Term getTerm() {
            return term;
        }

        public long getLastIndex() {
            return lastIndex;
        }
    }

    class RaftSnapshotMetadata {
        private final Term lastIncludedTerm;
        private final long lastIncludedIndex;
        private final ClusterConfiguration config;

        public RaftSnapshotMetadata(Term lastIncludedTerm, long lastIncludedIndex, ClusterConfiguration config) {
            this.lastIncludedTerm = lastIncludedTerm;
            this.lastIncludedIndex = lastIncludedIndex;
            this.config = config;
        }

        public Term getLastIncludedTerm() {
            return lastIncludedTerm;
        }

        public long getLastIncludedIndex() {
            return lastIncludedIndex;
        }

        public ClusterConfiguration getConfig() {
            return config;
        }
    }

    class RaftSnapshot {
        private final RaftSnapshotMetadata meta;
        private final Object data;

        public RaftSnapshot(RaftSnapshotMetadata meta, Object data) {
            this.meta = meta;
            this.data = data;
        }

        public RaftSnapshotMetadata getMeta() {
            return meta;
        }

        public Object getData() {
            return data;
        }

        public LogEntry toEntry() {
            return new LogEntry(this, meta.lastIncludedTerm, meta.lastIncludedIndex, Optional.empty());
        }
    }

    class LeaderIs {
        private final Optional<DiscoveryNode> ref;
        private final Optional<Object> msg;

        public LeaderIs(Optional<DiscoveryNode> ref, Optional<Object> msg) {
            this.ref = ref;
            this.msg = msg;
        }

        public Optional<DiscoveryNode> getRef() {
            return ref;
        }

        public Optional<Object> getMsg() {
            return msg;
        }
    }

    class WhoIsTheLeader {}

    class SendHeartbeat {}
    class AskForState {}
    class IAmInState {
        private final RaftState state;

        public IAmInState(RaftState state) {
            this.state = state;
        }

        public RaftState getState() {
            return state;
        }
    }

    class ChangeConfiguration {
        private final ClusterConfiguration newConf;

        public ChangeConfiguration(ClusterConfiguration newConf) {
            this.newConf = newConf;
        }

        public ClusterConfiguration getNewConf() {
            return newConf;
        }
    }
    class RequestConfiguration {}
}
