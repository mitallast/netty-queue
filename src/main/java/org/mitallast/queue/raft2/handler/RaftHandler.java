package org.mitallast.queue.raft2.handler;

import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.raft2.Raft;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.raft2.cluster.*;
import org.mitallast.queue.transport.TransportController;

public class RaftHandler extends AbstractComponent {

    public RaftHandler(Settings settings, TransportController transportController, Raft raft) {
        super(settings);

        transportController.registerMessageHandler(RaftMemberAdded.class, raft::receive);
        transportController.registerMessageHandler(RaftMemberRemoved.class, raft::receive);
        transportController.registerMessageHandler(RaftMembersDiscoveryRequest.class, raft::receive);
        transportController.registerMessageHandler(RaftMembersDiscoveryResponse.class, raft::receive);
        transportController.registerMessageHandler(RaftMembersDiscoveryTimeout.class, raft::receive);
    }
}
