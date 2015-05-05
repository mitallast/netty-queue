package org.mitallast.queue.action.cluster.disconnect;

import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.action.ActionType;
import org.mitallast.queue.cluster.DiscoveryNode;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.validation.ValidationBuilder;

import java.io.IOException;

public class ClusterDisconnectRequest extends ActionRequest {

    private DiscoveryNode discoveryNode;

    public DiscoveryNode getDiscoveryNode() {
        return discoveryNode;
    }

    public void setDiscoveryNode(DiscoveryNode discoveryNode) {
        this.discoveryNode = discoveryNode;
    }

    @Override
    public ActionType actionType() {
        return ActionType.CLUSTER_DISCONNECT;
    }

    @Override
    public ValidationBuilder validate() {
        return ValidationBuilder.builder().missing("discoveryNode", discoveryNode);
    }

    @Override
    public void readFrom(StreamInput stream) throws IOException {
        discoveryNode = new DiscoveryNode();
        discoveryNode.readFrom(stream);
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        discoveryNode.writeTo(stream);
    }
}
