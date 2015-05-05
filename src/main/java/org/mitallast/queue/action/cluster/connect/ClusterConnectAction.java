package org.mitallast.queue.action.cluster.connect;

import org.mitallast.queue.action.AbstractAction;
import org.mitallast.queue.action.ActionType;
import org.mitallast.queue.common.concurrent.Listener;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.transport.TransportController;

public class ClusterConnectAction extends AbstractAction<ClusterConnectRequest, ClusterConnectResponse> {

    public ClusterConnectAction(Settings settings, TransportController controller) {
        super(settings, controller);
    }

    @Override
    protected void executeInternal(ClusterConnectRequest request, Listener<ClusterConnectResponse> listener) {
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
