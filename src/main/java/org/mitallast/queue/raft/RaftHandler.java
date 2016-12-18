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

        transportController.registerMessageHandler(AppendEntries.class, raft::apply);
        transportController.registerMessageHandler(AppendRejected.class, raft::apply);
        transportController.registerMessageHandler(AppendSuccessful.class, raft::apply);

        transportController.registerMessageHandler(AddServer.class, raft::apply);
        transportController.registerMessageHandler(AddServerResponse.class, raft::apply);
        transportController.registerMessageHandler(RemoveServer.class, raft::apply);
        transportController.registerMessageHandler(RemoveServerResponse.class, raft::apply);

        transportController.registerMessageHandler(ClientMessage.class, raft::apply);

        transportController.registerMessageHandler(InstallSnapshot.class, raft::apply);
        transportController.registerMessageHandler(InstallSnapshotRejected.class, raft::apply);
        transportController.registerMessageHandler(InstallSnapshotSuccessful.class, raft::apply);

        transportController.registerMessageHandler(RequestVote.class, raft::apply);
        transportController.registerMessageHandler(VoteCandidate.class, raft::apply);
        transportController.registerMessageHandler(DeclineCandidate.class, raft::apply);
    }
}
