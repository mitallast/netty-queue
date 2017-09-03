package org.mitallast.queue.raft

import com.google.inject.AbstractModule
import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.raft.cluster.ClusterConfiguration
import org.mitallast.queue.raft.cluster.ClusterDiscovery
import org.mitallast.queue.raft.cluster.JointConsensusClusterConfiguration
import org.mitallast.queue.raft.cluster.StableClusterConfiguration
import org.mitallast.queue.raft.persistent.FilePersistentService
import org.mitallast.queue.raft.persistent.PersistentService
import org.mitallast.queue.raft.protocol.*
import org.mitallast.queue.raft.resource.ResourceRegistry

class RaftModule : AbstractModule() {

    override fun configure() {
        bind(Raft::class.java).asEagerSingleton()
        bind(RaftHandler::class.java).asEagerSingleton()
        bind(DefaultRaftContext::class.java).asEagerSingleton()
        bind(FilePersistentService::class.java).asEagerSingleton()
        bind(ResourceRegistry::class.java).asEagerSingleton()

        bind(ClusterDiscovery::class.java).asEagerSingleton()
        bind(PersistentService::class.java).to(FilePersistentService::class.java)

        bind(RaftContext::class.java).to(DefaultRaftContext::class.java)

    }

    companion object {
        init {
            Codec.register(200, AddServer::class.java, AddServer.codec)
            Codec.register(201, AddServerResponse::class.java, AddServerResponse.codec)
            Codec.register(202, AppendEntries::class.java, AppendEntries.codec)
            Codec.register(203, AppendRejected::class.java, AppendRejected.codec)
            Codec.register(204, AppendSuccessful::class.java, AppendSuccessful.codec)
            Codec.register(205, ClientMessage::class.java, ClientMessage.codec)
            Codec.register(206, DeclineCandidate::class.java, DeclineCandidate.codec)
            Codec.register(207, InstallSnapshot::class.java, InstallSnapshot.codec)
            Codec.register(208, InstallSnapshotRejected::class.java, InstallSnapshotRejected.codec)
            Codec.register(209, InstallSnapshotSuccessful::class.java, InstallSnapshotSuccessful.codec)
            Codec.register(210, LogEntry::class.java, LogEntry.codec)
            Codec.register(211, Noop::class.java, Noop.codec)
            Codec.register(212, RaftSnapshot::class.java, RaftSnapshot.codec)
            Codec.register(213, RaftSnapshotMetadata::class.java, RaftSnapshotMetadata.codec)
            Codec.register(214, RemoveServer::class.java, RemoveServer.codec)
            Codec.register(215, RemoveServerResponse::class.java, RemoveServerResponse.codec)
            Codec.register(216, RequestVote::class.java, RequestVote.codec)
            Codec.register(217, VoteCandidate::class.java, VoteCandidate.codec)

            Codec.register(218, ClusterConfiguration::class.java, ClusterConfiguration.codec)
            Codec.register(219, JointConsensusClusterConfiguration::class.java, JointConsensusClusterConfiguration.codec)
            Codec.register(220, StableClusterConfiguration::class.java, StableClusterConfiguration.codec)
        }
    }
}
