package org.mitallast.queue.crdt.routing.fsm;

import javaslang.collection.Set;
import org.mitallast.queue.common.codec.Codec;
import org.mitallast.queue.common.codec.Message;
import org.mitallast.queue.transport.DiscoveryNode;


public class UpdateMembers implements Message {
    public static final Codec<UpdateMembers> codec = Codec.of(
        UpdateMembers::new,
        UpdateMembers::members,
        Codec.setCodec(DiscoveryNode.codec)
    );

    private final Set<DiscoveryNode> members;

    public UpdateMembers(Set<DiscoveryNode> members) {
        this.members = members;
    }

    public Set<DiscoveryNode> members() {
        return members;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        UpdateMembers that = (UpdateMembers) o;

        return members.equals(that.members);
    }

    @Override
    public int hashCode() {
        return members.hashCode();
    }

    @Override
    public String toString() {
        return "UpdateMembers{" +
            "members=" + members +
            '}';
    }
}
