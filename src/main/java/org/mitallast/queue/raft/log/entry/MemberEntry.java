package org.mitallast.queue.raft.log.entry;

import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;

public abstract class MemberEntry<E extends MemberEntry<E>> extends RaftLogEntry<E> {
    protected final DiscoveryNode member;

    public MemberEntry(long index, long term, DiscoveryNode member) {
        super(index, term);
        this.member = member;
    }

    public final DiscoveryNode getMember() {
        return member;
    }

    public static abstract class MemberBuilder<B extends MemberBuilder<B, E>, E extends MemberEntry> extends Builder<B, E> {
        protected DiscoveryNode member;

        public B from(E entry) {
            member = entry.member;
            return super.from(entry);
        }

        @SuppressWarnings("unchecked")
        public final B setMember(DiscoveryNode member) {
            this.member = member;
            return (B) this;
        }

        @Override
        public void readFrom(StreamInput stream) throws IOException {
            super.readFrom(stream);
            member = stream.readStreamable(DiscoveryNode::new);
        }

        @Override
        public void writeTo(StreamOutput stream) throws IOException {
            super.writeTo(stream);
            stream.writeStreamable(member);
        }
    }
}
