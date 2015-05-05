package org.mitallast.queue.action.cluster.connect;

import com.google.inject.Inject;
import org.mitallast.queue.action.AbstractAction;
import org.mitallast.queue.action.ActionType;
import org.mitallast.queue.common.concurrent.Listener;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.transport.TransportController;
import org.mitallast.queue.transport.TransportService;

public class ClusterConnectAction extends AbstractAction<ClusterConnectRequest, ClusterConnectResponse> {

    private final TransportService transportService;

    @Inject
    public ClusterConnectAction(Settings settings, TransportController controller, TransportService transportService) {
        super(settings, controller);
        this.transportService = transportService;
    }

    @Override
    protected void executeInternal(ClusterConnectRequest request, Listener<ClusterConnectResponse> listener) {
        logger.info("received connect request {}", request.getDiscoveryNode());
        transportService.connectToNode(request.getDiscoveryNode());
        ClusterConnectResponse response = new ClusterConnectResponse();
        response.setConnected(true);
        listener.onResponse(response);
    }

    @Override
    public ActionType getActionId() {
        return ActionType.CLUSTER_CONNECT;
    }

    @Override
    public ClusterConnectRequest createRequest() {
        return new ClusterConnectRequest();
    }
}
