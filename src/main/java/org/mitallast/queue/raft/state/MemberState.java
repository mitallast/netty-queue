package org.mitallast.queue.raft.state;

import org.mitallast.queue.transport.DiscoveryNode;

public class MemberState {
    private final DiscoveryNode node;
    private final Type type;
    private volatile long version;
    private volatile int index;
    private volatile long matchIndex;
    private volatile long nextIndex;

    public MemberState(DiscoveryNode node, Type type) {
        this.node = node;
        this.type = type;
    }

    public DiscoveryNode getNode() {
        return node;
    }

    public Type getType() {
        return type;
    }

    public long getVersion() {
        return version;
    }

    public MemberState setVersion(long version) {
        this.version = version;
        return this;
    }

    public int getIndex() {
        return index;
    }

    public MemberState setIndex(int index) {
        this.index = index;
        return this;
    }

    public long getMatchIndex() {
        return matchIndex;
    }

    public MemberState setMatchIndex(long matchIndex) {
        this.matchIndex = matchIndex;
        return this;
    }

    public long getNextIndex() {
        return nextIndex;
    }

    public MemberState setNextIndex(long nextIndex) {
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

    public enum Type {
        PASSIVE, ACTIVE
    }
}
