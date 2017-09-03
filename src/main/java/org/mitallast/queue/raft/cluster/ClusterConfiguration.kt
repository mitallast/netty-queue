package org.mitallast.queue.raft.cluster

import javaslang.collection.HashSet
import javaslang.collection.Set
import org.mitallast.queue.common.codec.Codec
import org.mitallast.queue.common.codec.Message
import org.mitallast.queue.transport.DiscoveryNode

import java.io.DataInput
import java.io.DataOutput

interface ClusterConfiguration : Message {

    val members: Set<DiscoveryNode>

    val isTransitioning: Boolean

    fun transitionTo(newConfiguration: ClusterConfiguration): ClusterConfiguration

    fun transitionToStable(): ClusterConfiguration

    fun containsOnNewState(member: DiscoveryNode): Boolean

    companion object {
        val codec: Codec<ClusterConfiguration> = ClusterConfigurationCodec
    }
}

internal object ClusterConfigurationCodec : Codec<ClusterConfiguration> {
    override fun read(stream: DataInput): ClusterConfiguration {
        val isTransitioning = stream.readBoolean()
        return if (isTransitioning) {
            JointConsensusClusterConfiguration.codec.read(stream)
        } else {
            StableClusterConfiguration.codec.read(stream)
        }
    }

    override fun write(stream: DataOutput, value: ClusterConfiguration) {
        val isTransitioning = value.isTransitioning
        stream.writeBoolean(isTransitioning)
        if (isTransitioning) {
            JointConsensusClusterConfiguration.codec.write(stream, value as JointConsensusClusterConfiguration)
        } else {
            StableClusterConfiguration.codec.write(stream, value as StableClusterConfiguration)
        }
    }
}

data class JointConsensusClusterConfiguration(
        val oldMembers: Set<DiscoveryNode>,
        val newMembers: Set<DiscoveryNode>
) : ClusterConfiguration {
    override val members: Set<DiscoveryNode> = oldMembers.addAll(newMembers)
    override val isTransitioning = true

    override fun transitionTo(newConfiguration: ClusterConfiguration): ClusterConfiguration {
        throw IllegalStateException("Cannot start another configuration transition, already in progress")
    }

    override fun transitionToStable(): ClusterConfiguration {
        return StableClusterConfiguration(newMembers)
    }

    override fun containsOnNewState(member: DiscoveryNode): Boolean = newMembers.contains(member)

    companion object {
        val codec = Codec.of(
                ::JointConsensusClusterConfiguration,
                JointConsensusClusterConfiguration::oldMembers,
                JointConsensusClusterConfiguration::newMembers,
                Codec.setCodec(DiscoveryNode.codec),
                Codec.setCodec(DiscoveryNode.codec)
        )
    }
}

data class StableClusterConfiguration(override val members: Set<DiscoveryNode>) : ClusterConfiguration {

    constructor(vararg members: DiscoveryNode) : this(HashSet.of<DiscoveryNode>(*members))

    override val isTransitioning: Boolean = false

    override fun transitionTo(newConfiguration: ClusterConfiguration): ClusterConfiguration {
        return JointConsensusClusterConfiguration(members, newConfiguration.members)
    }

    override fun transitionToStable(): ClusterConfiguration = this

    override fun containsOnNewState(member: DiscoveryNode): Boolean = members.contains(member)

    companion object {
        val codec = Codec.of(
                ::StableClusterConfiguration,
                StableClusterConfiguration::members,
                Codec.setCodec(DiscoveryNode.codec)
        )
    }
}