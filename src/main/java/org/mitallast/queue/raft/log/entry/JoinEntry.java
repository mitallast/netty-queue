package org.mitallast.queue.raft.log.entry;

import org.mitallast.queue.transport.DiscoveryNode;

public class JoinEntry extends MemberEntry<JoinEntry> {
    public JoinEntry(long index, long term, DiscoveryNode member) {
        super(index, term, member);
    }

    @Override
    public String toString() {
        return "JoinEntry{" +
            "index=" + index +
            ", term=" + term +
            ", member=" + member +
            "}";
    }

    @Override
    public Builder toBuilder() {
        return new Builder().from(this);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends MemberBuilder<Builder, JoinEntry> {

        public JoinEntry build() {
            return new JoinEntry(index, term, member);
        }
    }
}
