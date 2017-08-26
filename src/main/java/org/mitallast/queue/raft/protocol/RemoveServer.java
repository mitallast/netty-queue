package org.mitallast.queue.raft.protocol;

import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;
import org.mitallast.queue.transport.DiscoveryNode;

public class RemoveServer implements Message {
    public static final Codec<RemoveServer> codec = Codec.of(
        RemoveServer::new,
        RemoveServer::getMember,
        DiscoveryNode.codec
    );

    private final DiscoveryNode member;

    public RemoveServer(DiscoveryNode member) {
        this.member = member;
    }

    public DiscoveryNode getMember() {
        return member;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RemoveServer that = (RemoveServer) o;

        return member.equals(that.member);
    }

    @Override
    public int hashCode() {
        return member.hashCode();
    }

    @Override
    public String toString() {
        return "RemoveServer{" +
            "member=" + member +
            '}';
    }
}