package org.mitallast.queue.action.cluster.disconnect;

import com.google.inject.Inject;
import org.mitallast.queue.action.AbstractAction;
import org.mitallast.queue.action.ActionType;
import org.mitallast.queue.common.concurrent.Listener;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.transport.TransportController;
import org.mitallast.queue.transport.TransportService;

public class ClusterDisconnectAction extends AbstractAction<ClusterDisconnectRequest, ClusterDisconnectResponse> {

    private final TransportService transportService;

    @Inject
    public ClusterDisconnectAction(Settings settings, TransportController controller, TransportService transportService) {
        super(settings, controller);
        this.transportService = transportService;
    }

    @Override
    protected void executeInternal(ClusterDisconnectRequest request, Listener<ClusterDisconnectResponse> listener) {
        logger.info("received disconnect request {}", request.getDiscoveryNode());
        ClusterDisconnectResponse response = new ClusterDisconnectResponse();
        listener.onResponse(response);
        transportService.disconnectFromNode(request.getDiscoveryNode());
    }

    @Override
    public ActionType getActionId() {
        return ActionType.CLUSTER_DISCONNECT;
    }

    @Override
    public ClusterDisconnectRequest createRequest() {
        return new ClusterDisconnectRequest();
    }
}
