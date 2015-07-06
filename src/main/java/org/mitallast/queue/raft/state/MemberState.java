package org.mitallast.queue.raft.state;

import org.mitallast.queue.raft.cluster.Member;
import org.mitallast.queue.transport.DiscoveryNode;

class MemberState {
    private final DiscoveryNode node;
    private final Member.Type type;
    private volatile long version;
    private volatile int index;
    private volatile long matchIndex;
    private volatile long nextIndex;

    public MemberState(DiscoveryNode node, Member.Type type) {
        this.node = node;
        this.type = type;
    }

    DiscoveryNode getNode() {
        return node;
    }

    Member.Type getType() {
        return type;
    }

    public long getVersion() {
        return version;
    }

    MemberState setVersion(long version) {
        this.version = version;
        return this;
    }

    public int getIndex() {
        return index;
    }

    MemberState setIndex(int index) {
        this.index = index;
        return this;
    }

    long getMatchIndex() {
        return matchIndex;
    }

    MemberState setMatchIndex(long matchIndex) {
        this.matchIndex = matchIndex;
        return this;
    }

    long getNextIndex() {
        return nextIndex;
    }

    MemberState setNextIndex(long nextIndex) {
        this.nextIndex = nextIndex;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MemberState that = (MemberState) o;

        return node.equals(that.node);

    }

    @Override
    public int hashCode() {
        return node.hashCode();
    }

    @Override
    public String toString() {
        return "MemberState{node=" + node + '}';
    }
}
