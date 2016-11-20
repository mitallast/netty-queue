package org.mitallast.queue.raft;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.raft.cluster.*;
import org.mitallast.queue.raft.protocol.*;
import org.mitallast.queue.transport.TransportController;

public class RaftHandler extends AbstractComponent {

    @Inject
    public RaftHandler(Config config, TransportController transportController, Raft raft) {
        super(config.getConfig("raft"), RaftHandler.class);

        transportController.registerMessageHandler(AppendEntries.class, raft::receive);
        transportController.registerMessageHandler(AppendRejected.class, raft::receive);
        transportController.registerMessageHandler(AppendSuccessful.class, raft::receive);

        transportController.registerMessageHandler(ChangeConfiguration.class, raft::receive);
        transportController.registerMessageHandler(JointRequest.class, raft::receive);

        transportController.registerMessageHandler(ClientMessage.class, raft::receive);

        transportController.registerMessageHandler(InstallSnapshot.class, raft::receive);
        transportController.registerMessageHandler(InstallSnapshotRejected.class, raft::receive);
        transportController.registerMessageHandler(InstallSnapshotSuccessful.class, raft::receive);

        transportController.registerMessageHandler(RequestVote.class, raft::receive);
        transportController.registerMessageHandler(VoteCandidate.class, raft::receive);
        transportController.registerMessageHandler(DeclineCandidate.class, raft::receive);
    }
}
