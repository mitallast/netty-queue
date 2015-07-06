package org.mitallast.queue.raft.cluster;

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import org.mitallast.queue.Version;
import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.common.concurrent.Futures;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.raft.RaftException;
import org.mitallast.queue.raft.action.RaftResponse;
import org.mitallast.queue.raft.action.ResponseStatus;
import org.mitallast.queue.transport.DiscoveryNode;
import org.mitallast.queue.transport.TransportListener;
import org.mitallast.queue.transport.TransportService;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class TransportCluster extends AbstractCluster implements TransportListener {

    private final TransportService transportService;

    @Inject
    protected TransportCluster(Settings settings, TransportService transportService) {
        super(settings, new TransportMember(transportService.localNode(), Member.Type.ACTIVE, transportService), ImmutableList.of());
        this.transportService = transportService;
        this.transportService.addListener(this);

        String[] nodes = this.componentSettings.getAsArray("nodes");
        for (String node : nodes) {
            HostAndPort hostAndPort = HostAndPort.fromString(node);
            DiscoveryNode discoveryNode = new DiscoveryNode(node, hostAndPort, Version.CURRENT);
            Member member = createMember(discoveryNode);
            member.type = Member.Type.ACTIVE;
            addMember(member);
        }
    }

    @Override
    public void connected(HostAndPort address) {
        logger.info("node connected {}", address);
    }

    @Override
    public void connected(DiscoveryNode node) {
        logger.info("node connected {}", node);
        addMember(node);
    }

    @Override
    public void disconnected(HostAndPort address) {
        logger.info("node disconnected {}", address);
    }

    @Override
    public void disconnected(DiscoveryNode node) {
        logger.info("node disconnected {}", node);
        removeMember(node);
    }

    @Override
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
        public <T extends ActionRequest, R extends RaftResponse> CompletableFuture<R> send(T message) {
            CompletableFuture<R> future = Futures.future();
            // map response error status as exception object
            transportService.client(node().address()).<T, R>send(message).whenComplete((response, throwable) -> {
                if (throwable != null) {
                    future.completeExceptionally(throwable);
                } else if (response.status() == ResponseStatus.ERROR) {
                    future.completeExceptionally(new RaftException(response.error()));
                } else {
                    future.complete(response);
                }
            });
            return future;
        }

        @Override
        public <T, U> CompletableFuture<U> send(Class<? super T> type, T message) {
            throw new RuntimeException("Not implemented");
        }

        @Override
        public <T, U> CompletableFuture<U> send(String topic, T message) {
            throw new RuntimeException("Not implemented");
        }

        @Override
        public String toString() {
            return "TransportMember{node=" + node() + "}";
        }
    }
}