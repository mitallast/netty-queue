package org.mitallast.queue.raft.cluster;

import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import org.mitallast.queue.Version;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;
import org.mitallast.queue.common.concurrent.Futures;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.transport.DiscoveryNode;
import org.mitallast.queue.transport.TransportService;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TransportCluster extends AbstractLifecycleComponent {
    private final TransportService transportService;
    private final ConcurrentMap<DiscoveryNode, Member> members = new ConcurrentHashMap<>();

    @Inject
    protected TransportCluster(Settings settings, TransportService transportService) {
        super(settings);
        this.transportService = transportService;

        String[] nodes = componentSettings.getAsArray("nodes");
        for (String node : nodes) {
            HostAndPort hostAndPort = HostAndPort.fromString(node);
            DiscoveryNode discoveryNode = new DiscoveryNode(node, hostAndPort, Version.CURRENT);
            Member member = new Member(discoveryNode, Member.Type.ACTIVE);
            members.put(member.node(), member);
        }
    }

    public synchronized void addMember(DiscoveryNode node) {
        members.computeIfAbsent(node, $ -> new Member(node, Member.Type.PASSIVE));
    }

    public synchronized CompletableFuture<Void> removeMember(DiscoveryNode node) {
        members.remove(node);
        return Futures.complete(null);
    }

    public CompletionStage<Void> configure(Collection<DiscoveryNode> discoveryNodes) {
        discoveryNodes.forEach(this::addMember);
        return Futures.complete(null);
    }

    @Override
    protected void doStart() throws IOException {
        for (Member member : members.values()) {
            transportService.connectToNode(member.node().address());
        }
    }

    @Override
    protected void doStop() throws IOException {
    }

    @Override
    protected void doClose() throws IOException {
    }
}