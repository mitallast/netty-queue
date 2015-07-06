package org.mitallast.queue.raft.cluster;

import com.google.common.collect.ImmutableList;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;
import org.mitallast.queue.common.concurrent.Futures;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.transport.DiscoveryNode;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public abstract class AbstractMembers extends AbstractLifecycleComponent implements Members {
    protected final Map<DiscoveryNode, Member> members = new ConcurrentHashMap<>();
    protected final CopyOnWriteArrayList<MembershipListener> membershipListeners = new CopyOnWriteArrayList<>();

    protected AbstractMembers(Settings settings, Collection<Member> remoteMembers) {
        super(settings);
        remoteMembers.forEach(member -> this.members.put(member.node(), member));
    }

    public synchronized void addMember(Member member) {
        if (!members.containsKey(member.node())) {
            members.put(member.node(), member);
        }
    }

    public synchronized void addMember(DiscoveryNode node) {
        if (!members.containsKey(node)) {
            Member member = createMember(node);
            member.type = Member.Type.PASSIVE;
            members.put(member.node(), member);
            membershipListeners.forEach(l -> l.memberJoined(member));
        }
    }

    protected abstract Member createMember(DiscoveryNode discoveryNode);

    public synchronized CompletableFuture<Void> removeMember(DiscoveryNode node) {
        if (members.containsKey(node)) {
            Member member = members.remove(node);
            membershipListeners.forEach(l -> l.memberLeft(member));
        }
        return Futures.complete(null);
    }

    @Override
    public synchronized void configureMember(DiscoveryNode node, Member.Type type) {
        Member member = members.get(node);
        if (member != null) {
            member.type = type;
        }
    }

    @Override
    public List<Member> members() {
        return ImmutableList.copyOf(members.values());
    }

    @Override
    public Member member(DiscoveryNode node) {
        return members.get(node);
    }

    @Override
    public CompletionStage<Void> configure(DiscoveryNode[] discoveryNodes) {
        return Futures.completeExceptionally(new RuntimeException("Not implemented"));
    }

    @Override
    public void addListener(MembershipListener listener) {
        membershipListeners.add(listener);
    }

    @Override
    public void removeListener(MembershipListener listener) {
        membershipListeners.remove(listener);
    }
}
