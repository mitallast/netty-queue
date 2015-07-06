package org.mitallast.queue.raft.log.entry;

import org.mitallast.queue.transport.DiscoveryNode;

public class LeaveEntry extends MemberEntry<LeaveEntry> {

    public LeaveEntry(long index, long term, DiscoveryNode member) {
        super(index, term, member);
    }

    @Override
    public String toString() {
        return "LeaveEntry{" +
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

    public static class Builder extends MemberBuilder<Builder, LeaveEntry> {
        public LeaveEntry build() {
            return new LeaveEntry(index, term, member);
        }
    }
}
