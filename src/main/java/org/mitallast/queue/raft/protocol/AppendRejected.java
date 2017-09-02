package org.mitallast.queue.raft.protocol;

import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;
import org.mitallast.queue.transport.DiscoveryNode;

public class AppendRejected implements Message {
    public static final Codec<AppendRejected> codec = Codec.Companion.of(
        AppendRejected::new,
        AppendRejected::getMember,
        AppendRejected::getTerm,
        AppendRejected::getLastIndex,
        DiscoveryNode.codec,
        Codec.Companion.longCodec(),
        Codec.Companion.longCodec()
    );

    private final DiscoveryNode member;
    private final long term;
    private final long lastIndex;

    public AppendRejected(DiscoveryNode member, long term, long lastIndex) {
        this.member = member;
        this.term = term;
        this.lastIndex = lastIndex;
    }

    public DiscoveryNode getMember() {
        return member;
    }

    public long getTerm() {
        return term;
    }

    public long getLastIndex() {
        return lastIndex;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AppendRejected that = (AppendRejected) o;

        if (term != that.term) return false;
        if (lastIndex != that.lastIndex) return false;
        return member.equals(that.member);
    }

    @Override
    public int hashCode() {
        int result = member.hashCode();
        result = 31 * result + (int) (term ^ (term >>> 32));
        result = 31 * result + (int) (lastIndex ^ (lastIndex >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "AppendRejected{" +
            "member=" + member +
            ", term=" + term +
            ", lastIndex=" + lastIndex +
            '}';
    }
}
