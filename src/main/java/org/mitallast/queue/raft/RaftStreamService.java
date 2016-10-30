package org.mitallast.queue.raft;

import com.google.inject.Inject;
import org.mitallast.queue.common.stream.StreamService;
import org.mitallast.queue.raft.cluster.*;
import org.mitallast.queue.raft.protocol.*;

public class RaftStreamService {

    @Inject
    public RaftStreamService(StreamService streamService) {
        streamService.register(JointConsensusClusterConfiguration.class, JointConsensusClusterConfiguration::new, 100);
        streamService.register(RaftMemberAdded.class, RaftMemberAdded::new, 101);
        streamService.register(RaftMemberRemoved.class, RaftMemberRemoved::new, 102);
        streamService.register(RaftMembersDiscoveryRequest.class, RaftMembersDiscoveryRequest::read, 103);
        streamService.register(RaftMembersDiscoveryResponse.class, RaftMembersDiscoveryResponse::new, 104);
        streamService.register(RaftMembersDiscoveryTimeout.class, RaftMembersDiscoveryTimeout::read, 105);
        streamService.register(StableClusterConfiguration.class, StableClusterConfiguration::new, 106);


        streamService.register(AppendEntries.class, AppendEntries::new, 200);
        streamService.register(AppendRejected.class, AppendRejected::new, 201);
        streamService.register(AppendSuccessful.class, AppendSuccessful::new, 202);
        streamService.register(BeginElection.class, BeginElection::read, 204);
        streamService.register(ChangeConfiguration.class, ChangeConfiguration::new, 205);
        streamService.register(ClientMessage.class, ClientMessage::new, 206);
        streamService.register(DeclineCandidate.class, DeclineCandidate::new, 207);
        streamService.register(ElectedAsLeader.class, ElectedAsLeader::read, 208);
        streamService.register(ElectionTimeout.class, ElectionTimeout::read, 209);
        streamService.register(InitLogSnapshot.class, InitLogSnapshot::read, 211);
        streamService.register(InstallSnapshot.class, InstallSnapshot::new, 212);
        streamService.register(InstallSnapshotRejected.class, InstallSnapshotRejected::new, 213);
        streamService.register(InstallSnapshotSuccessful.class, InstallSnapshotSuccessful::new, 214);
        streamService.register(LogEntry.class, LogEntry::new, 216);
        streamService.register(RaftSnapshot.class, RaftSnapshot::new, 217);
        streamService.register(RaftSnapshotMetadata.class, RaftSnapshotMetadata::new, 218);
        streamService.register(RequestConfiguration.class, RequestConfiguration::read, 219);
        streamService.register(RequestVote.class, RequestVote::new, 220);
        streamService.register(SendHeartbeat.class, SendHeartbeat::read, 221);
        streamService.register(VoteCandidate.class, VoteCandidate::new, 222);
    }
}
