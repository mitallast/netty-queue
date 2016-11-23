package org.mitallast.queue.raft;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;
import org.mitallast.queue.common.stream.StreamableRegistry;
import org.mitallast.queue.raft.cluster.JointConsensusClusterConfiguration;
import org.mitallast.queue.raft.cluster.StableClusterConfiguration;
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

        streamableBinder.addBinding().toInstance(StreamableRegistry.of(AppendEntries.class, AppendEntries::new, 200));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(AppendRejected.class, AppendRejected::new, 201));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(AppendSuccessful.class, AppendSuccessful::new, 202));

        streamableBinder.addBinding().toInstance(StreamableRegistry.of(ClientMessage.class, ClientMessage::new, 210));

        streamableBinder.addBinding().toInstance(StreamableRegistry.of(ElectionTimeout.class, ElectionTimeout::read, 220));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(BeginElection.class, BeginElection::read, 221));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(RequestVote.class, RequestVote::new, 223));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(VoteCandidate.class, VoteCandidate::new, 225));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(DeclineCandidate.class, DeclineCandidate::new, 226));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(ElectedAsLeader.class, ElectedAsLeader::read, 222));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(SendHeartbeat.class, SendHeartbeat::read, 224));

        streamableBinder.addBinding().toInstance(StreamableRegistry.of(LogEntry.class, LogEntry::new, 230));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(Noop.class, Noop::read, 231));

        streamableBinder.addBinding().toInstance(StreamableRegistry.of(RaftSnapshot.class, RaftSnapshot::new, 240));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(RaftSnapshotMetadata.class, RaftSnapshotMetadata::new, 241));

        streamableBinder.addBinding().toInstance(StreamableRegistry.of(InitLogSnapshot.class, InitLogSnapshot::read, 260));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(InstallSnapshot.class, InstallSnapshot::new, 261));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(InstallSnapshotRejected.class, InstallSnapshotRejected::new, 262));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(InstallSnapshotSuccessful.class, InstallSnapshotSuccessful::new, 263));

        streamableBinder.addBinding().toInstance(StreamableRegistry.of(JointConsensusClusterConfiguration.class, JointConsensusClusterConfiguration::new, 270));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(StableClusterConfiguration.class, StableClusterConfiguration::new, 271));

        streamableBinder.addBinding().toInstance(StreamableRegistry.of(AddServer.class, AddServer::new, 280));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(AddServerResponse.class, AddServerResponse::new, 281));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(RemoveServer.class, RemoveServer::new, 282));
        streamableBinder.addBinding().toInstance(StreamableRegistry.of(RemoveServerResponse.class, RemoveServerResponse::new, 283));
    }
}
