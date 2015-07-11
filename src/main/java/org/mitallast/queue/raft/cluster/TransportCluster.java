package org.mitallast.queue.raft.cluster;

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import org.mitallast.queue.Version;
import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;
import org.mitallast.queue.common.concurrent.Futures;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.transport.DiscoveryNode;
import org.mitallast.queue.transport.TransportService;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TransportCluster extends AbstractLifecycleComponent {
    private final TransportService transportService;
    private final Member localMember;
    private final ConcurrentMap<DiscoveryNode, Member> members = new ConcurrentHashMap<>();

    @Inject
    protected TransportCluster(Settings settings, TransportService transportService) {
        super(settings);
        this.transportService = transportService;
        this.localMember = new TransportMember(transportService.localNode(), Member.Type.ACTIVE, transportService);

        String[] nodes = componentSettings.getAsArray("nodes");
        for (String node : nodes) {
            HostAndPort hostAndPort = HostAndPort.fromString(node);
            DiscoveryNode discoveryNode = new DiscoveryNode(node, hostAndPort, Version.CURRENT);
            Member member = createMember(discoveryNode);
            member.type = Member.Type.ACTIVE;
            addMember(member);
        }
    }

    public Member member() {
        return localMember;
    }

    public synchronized void addMember(Member member) {
        members.putIfAbsent(member.node(), member);
    }

    public synchronized void addMember(DiscoveryNode node) {
        members.computeIfAbsent(node, this::createMember);
    }

    public synchronized CompletableFuture<Void> removeMember(DiscoveryNode node) {
        members.remove(node);
        return Futures.complete(null);
    }

    public List<Member> members() {
        return ImmutableList.copyOf(members.values());
    }

    public Member member(DiscoveryNode node) {
        return members.get(node);
    }

    public CompletionStage<Void> configure(Collection<DiscoveryNode> discoveryNodes) {
        discoveryNodes.forEach(this::addMember);
        return Futures.complete(null);
    }

    protected Member createMember(DiscoveryNode discoveryNode) {
        return new TransportMember(discoveryNode, Member.Type.PASSIVE, transportService);
    }

    @Override
    protected void doStart() throws IOException {
        for (Member member : members()) {
            transportService.connectToNode(member.node().address());
        }
    }

    @Override
    protected void doStop() throws IOException {
    }

    @Override
    protected void doClose() throws IOException {

    }

    private static class TransportMember extends Member {
        private final TransportService transportService;

        protected TransportMember(DiscoveryNode node, Type type, TransportService transportService) {
            super(node, type);
            this.transportService = transportService;
        }

        @Override
        public <T extends ActionRequest, R extends ActionResponse> CompletableFuture<R> send(T message) {
            return transportService.client(node().address()).<T, R>send(message);
        }

        @Override
        public String toString() {
            return "TransportMember{node=" + node() + "}";
        }
    }
}