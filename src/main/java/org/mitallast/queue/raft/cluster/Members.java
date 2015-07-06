package org.mitallast.queue.raft.cluster;

import org.mitallast.queue.transport.DiscoveryNode;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public interface Members {

    void addMember(DiscoveryNode discoveryNode);

    CompletableFuture<Void> removeMember(DiscoveryNode node);

    Member member(DiscoveryNode node);

    List<Member> members();

    void configureMember(DiscoveryNode node, Member.Type type);

    void addListener(MembershipListener listener);

    void removeListener(MembershipListener listener);

    CompletionStage<Void> configure(DiscoveryNode[] discoveryNodes);
}
