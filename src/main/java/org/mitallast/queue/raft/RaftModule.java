package org.mitallast.queue.raft;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;
import org.mitallast.queue.common.proto.ProtoRegistry;
import org.mitallast.queue.proto.raft.*;
import org.mitallast.queue.raft.discovery.ClusterDiscovery;
import org.mitallast.queue.raft.persistent.FilePersistentService;
import org.mitallast.queue.raft.persistent.PersistentService;
import org.mitallast.queue.raft.resource.ResourceRegistry;

public class RaftModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(Raft.class).asEagerSingleton();
        bind(RaftHandler.class).asEagerSingleton();
        bind(DefaultRaftContext.class).asEagerSingleton();
        bind(FilePersistentService.class).asEagerSingleton();

        bind(ClusterDiscovery.class).asEagerSingleton();
        bind(PersistentService.class).to(FilePersistentService.class);

        bind(RaftContext.class).to(DefaultRaftContext.class);

        bind(ResourceRegistry.class).asEagerSingleton();

        Multibinder<ProtoRegistry> protoBinder = Multibinder.newSetBinder(binder(), ProtoRegistry.class);

        protoBinder.addBinding().toInstance(new ProtoRegistry(1000, AddServer.getDescriptor(), AddServer.parser()));
        protoBinder.addBinding().toInstance(new ProtoRegistry(1001, AddServerResponse.getDescriptor(), AddServerResponse.parser()));
        protoBinder.addBinding().toInstance(new ProtoRegistry(1002, AppendEntries.getDescriptor(), AppendEntries.parser()));
        protoBinder.addBinding().toInstance(new ProtoRegistry(1003, AppendRejected.getDescriptor(), AppendRejected.parser()));
        protoBinder.addBinding().toInstance(new ProtoRegistry(1004, AppendSuccessful.getDescriptor(), AppendSuccessful.parser()));
        protoBinder.addBinding().toInstance(new ProtoRegistry(1005, ClientMessage.getDescriptor(), ClientMessage.parser()));
        protoBinder.addBinding().toInstance(new ProtoRegistry(1006, DeclineCandidate.getDescriptor(), DeclineCandidate.parser()));
        protoBinder.addBinding().toInstance(new ProtoRegistry(1007, DiscoveryNode.getDescriptor(), DiscoveryNode.parser()));
        protoBinder.addBinding().toInstance(new ProtoRegistry(1008, InstallSnapshot.getDescriptor(), InstallSnapshot.parser()));
        protoBinder.addBinding().toInstance(new ProtoRegistry(1009, InstallSnapshotRejected.getDescriptor(), InstallSnapshotRejected.parser()));
        protoBinder.addBinding().toInstance(new ProtoRegistry(1010, InstallSnapshotSuccessful.getDescriptor(), InstallSnapshotSuccessful.parser()));
        protoBinder.addBinding().toInstance(new ProtoRegistry(1011, ClusterConfiguration.getDescriptor(), ClusterConfiguration.parser()));
        protoBinder.addBinding().toInstance(new ProtoRegistry(1012, JointConsensusClusterConfiguration.getDescriptor(), JointConsensusClusterConfiguration.parser()));
        protoBinder.addBinding().toInstance(new ProtoRegistry(1013, StableClusterConfiguration.getDescriptor(), StableClusterConfiguration.parser()));
        protoBinder.addBinding().toInstance(new ProtoRegistry(1014, LogEntry.getDescriptor(), LogEntry.parser()));
        protoBinder.addBinding().toInstance(new ProtoRegistry(1015, Noop.getDescriptor(), Noop.parser()));
        protoBinder.addBinding().toInstance(new ProtoRegistry(1016, RaftSnapshot.getDescriptor(), RaftSnapshot.parser()));
        protoBinder.addBinding().toInstance(new ProtoRegistry(1017, RaftSnapshotMetadata.getDescriptor(), RaftSnapshotMetadata.parser()));
        protoBinder.addBinding().toInstance(new ProtoRegistry(1018, RemoveServer.getDescriptor(), RemoveServer.parser()));
        protoBinder.addBinding().toInstance(new ProtoRegistry(1019, RemoveServerResponse.getDescriptor(), RemoveServerResponse.parser()));
        protoBinder.addBinding().toInstance(new ProtoRegistry(1020, RequestVote.getDescriptor(), RequestVote.parser()));
        protoBinder.addBinding().toInstance(new ProtoRegistry(1021, VoteCandidate.getDescriptor(), VoteCandidate.parser()));
    }
}
