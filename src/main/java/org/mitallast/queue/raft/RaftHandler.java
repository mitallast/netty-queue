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

        // cluster
        transportController.registerMessageHandler(RaftMemberAdded.class, raft::receive);
        transportController.registerMessageHandler(RaftMemberRemoved.class, raft::receive);
        transportController.registerMessageHandler(RaftMembersDiscoveryRequest.class, raft::receive);
        transportController.registerMessageHandler(RaftMembersDiscoveryResponse.class, raft::receive);
        transportController.registerMessageHandler(RaftMembersDiscoveryTimeout.class, raft::receive);

        // raft protocol
        transportController.registerMessageHandler(AppendEntries.class, raft::receive);
        transportController.registerMessageHandler(AppendRejected.class, raft::receive);
        transportController.registerMessageHandler(AppendSuccessful.class, raft::receive);
        transportController.registerMessageHandler(AskForState.class, raft::receive);
        transportController.registerMessageHandler(BeginElection.class, raft::receive);
        transportController.registerMessageHandler(ChangeConfiguration.class, raft::receive);
        transportController.registerMessageHandler(ClientMessage.class, raft::receive);
        transportController.registerMessageHandler(DeclineCandidate.class, raft::receive);
        transportController.registerMessageHandler(ElectedAsLeader.class, raft::receive);
        transportController.registerMessageHandler(ElectionTimeout.class, raft::receive);
        transportController.registerMessageHandler(IAmInState.class, raft::receive);
        transportController.registerMessageHandler(InitLogSnapshot.class, raft::receive);
        transportController.registerMessageHandler(InstallSnapshot.class, raft::receive);
        transportController.registerMessageHandler(InstallSnapshotRejected.class, raft::receive);
        transportController.registerMessageHandler(InstallSnapshotSuccessful.class, raft::receive);
        transportController.registerMessageHandler(LeaderIs.class, raft::receive);
        transportController.registerMessageHandler(LogEntry.class, raft::receive);
        transportController.registerMessageHandler(RaftSnapshot.class, raft::receive);
        transportController.registerMessageHandler(RaftSnapshotMetadata.class, raft::receive);
        transportController.registerMessageHandler(RequestConfiguration.class, raft::receive);
        transportController.registerMessageHandler(RequestVote.class, raft::receive);
        transportController.registerMessageHandler(SendHeartbeat.class, raft::receive);
        transportController.registerMessageHandler(VoteCandidate.class, raft::receive);
        transportController.registerMessageHandler(WhoIsTheLeader.class, raft::receive);
    }
}
