package org.mitallast.queue.transport;

import com.google.common.collect.ImmutableList;
import org.mitallast.queue.cluster.DiscoveryNode;
import org.mitallast.queue.common.concurrent.futures.SmartFuture;
import org.mitallast.queue.common.event.EventListener;

public interface TransportService {

    DiscoveryNode localNode();

    void connectToNode(DiscoveryNode node);

    void disconnectFromNode(DiscoveryNode node);

    ImmutableList<DiscoveryNode> connectedNodes();

    SmartFuture<TransportFrame> sendRequest(DiscoveryNode node, TransportFrame frame);

    TransportClient client(DiscoveryNode node);

    void addNodeConnectedListener(EventListener<NodeConnectedEvent> listener);

    void removeNodeConnectedListener(EventListener<NodeConnectedEvent> listener);

    void addNodeDisconnectedListener(EventListener<NodeDisconnectedEvent> listener);

    void removeNodeDisconnectedListener(EventListener<NodeDisconnectedEvent> listener);

    public class NodeConnectedEvent {
        private final DiscoveryNode connectedNode;

        public NodeConnectedEvent(DiscoveryNode connectedNode) {
            this.connectedNode = connectedNode;
        }

        public DiscoveryNode getConnectedNode() {
            return connectedNode;
        }
    }

    public class NodeDisconnectedEvent {
        private final DiscoveryNode connectedNode;

        public NodeDisconnectedEvent(DiscoveryNode connectedNode) {
            this.connectedNode = connectedNode;
        }

        public DiscoveryNode getConnectedNode() {
            return connectedNode;
        }
    }
}
