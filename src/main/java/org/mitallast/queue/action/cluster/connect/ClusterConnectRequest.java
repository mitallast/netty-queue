package org.mitallast.queue.action.cluster.connect;

import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.action.ActionRequestValidationException;
import org.mitallast.queue.action.ActionType;
import org.mitallast.queue.cluster.DiscoveryNode;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;

import java.io.IOException;

import static org.mitallast.queue.action.ValidateActions.addValidationError;

public class ClusterConnectRequest extends ActionRequest {

    private DiscoveryNode discoveryNode;

    public DiscoveryNode getDiscoveryNode() {
        return discoveryNode;
    }

    public void setDiscoveryNode(DiscoveryNode discoveryNode) {
        this.discoveryNode = discoveryNode;
    }

    @Override
    public ActionType actionType() {
        return ActionType.CLUSTER_CONNECT;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (discoveryNode == null) {
            validationException = addValidationError("discoveryNode is missing", null);
        }
        return validationException;
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
