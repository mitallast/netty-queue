package org.mitallast.queue.raft.protocol;

import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;
import org.mitallast.queue.transport.DiscoveryNode;

public class AddServer implements Message {
    public static final Codec<AddServer> codec = Codec.Companion.of(
        AddServer::new,
        AddServer::getMember,
        DiscoveryNode.codec
    );

    private final DiscoveryNode member;

    public AddServer(DiscoveryNode member) {
        this.member = member;
    }

    public DiscoveryNode getMember() {
        return member;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AddServer addServer = (AddServer) o;

        return member.equals(addServer.member);
    }

    @Override
    public int hashCode() {
        return member.hashCode();
    }

    @Override
    public String toString() {
        return "AddServer{" + member + "}";
    }
}