package org.mitallast.queue.raft;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;
import org.mitallast.queue.common.stream.StreamableRegistry;
import org.mitallast.queue.raft.cluster.*;
import org.mitallast.queue.raft.discovery.ClusterDiscovery;
import org.mitallast.queue.raft.log.FileReplicatedLogProvider;
import org.mitallast.queue.raft.log.ReplicatedLog;
import org.mitallast.queue.raft.protocol.*;

public class RaftModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(Raft.class).asEagerSingleton();
        bind(RaftHandler.class).asEagerSingleton();
        bind(DefaultRaftContext.class).asEagerSingleton();

        bind(ClusterDiscovery.class).asEagerSingleton();
        bind(ReplicatedLog.class).toProvider(FileReplicatedLogProvider.class);

        bind(RaftContext.class).to(DefaultRaftContext.class);

        Multibinder<StreamableRegistry> streamableBinder = Multibinder.newSetBinder(binder(), StreamableRegistry.class);

        streamableBinder.addBinding().toInstance(StreamableRegistry.of(JointConsensusClusterConfiguration.class, JointConsensusClusterConfiguration::new, 100));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(RaftMemberAdded.class, RaftMemberAdded::new, 101));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(RaftMemberRemoved.class, RaftMemberRemoved::new, 102));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(RaftMembersDiscoveryRequest.class, RaftMembersDiscoveryRequest::new, 103));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(RaftMembersDiscoveryResponse.class, RaftMembersDiscoveryResponse::new, 104));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(RaftMembersDiscoveryTimeout.class, RaftMembersDiscoveryTimeout::read, 105));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(StableClusterConfiguration.class, StableClusterConfiguration::new, 106));


        streamableBinder.addBinding().toInstance(StreamableRegistry.of(AppendEntries.class, AppendEntries::new, 200));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(AppendRejected.class, AppendRejected::new, 201));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(AppendSuccessful.class, AppendSuccessful::new, 202));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(BeginElection.class, BeginElection::read, 204));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(ChangeConfiguration.class, ChangeConfiguration::new, 205));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(ClientMessage.class, ClientMessage::new, 206));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(DeclineCandidate.class, DeclineCandidate::new, 207));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(ElectedAsLeader.class, ElectedAsLeader::read, 208));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(ElectionTimeout.class, ElectionTimeout::read, 209));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(InitLogSnapshot.class, InitLogSnapshot::read, 211));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(InstallSnapshot.class, InstallSnapshot::new, 212));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(InstallSnapshotRejected.class, InstallSnapshotRejected::new, 213));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(InstallSnapshotSuccessful.class, InstallSnapshotSuccessful::new, 214));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(LogEntry.class, LogEntry::new, 216));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(RaftSnapshot.class, RaftSnapshot::new, 217));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(RaftSnapshotMetadata.class, RaftSnapshotMetadata::new, 218));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(RequestConfiguration.class, RequestConfiguration::new, 219));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(RequestVote.class, RequestVote::new, 220));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(SendHeartbeat.class, SendHeartbeat::read, 221));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(VoteCandidate.class, VoteCandidate::new, 222));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(Noop.class, Noop::read, 223));
    }
}
