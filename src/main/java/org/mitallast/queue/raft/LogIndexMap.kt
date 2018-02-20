package org.mitallast.queue.raft

import gnu.trove.list.array.TLongArrayList
import gnu.trove.map.TObjectLongMap
import gnu.trove.map.hash.TObjectLongHashMap
import io.vavr.collection.Set
import org.mitallast.queue.raft.cluster.ClusterConfiguration
import org.mitallast.queue.raft.cluster.JointConsensusClusterConfiguration
import org.mitallast.queue.transport.DiscoveryNode

class LogIndexMap(defaultIndex: Long) {
    private val backing: TObjectLongMap<DiscoveryNode>
    private val indexes = TLongArrayList(64)

    init {
        this.backing = TObjectLongHashMap(64, 0.5f, defaultIndex)
    }

    fun decrementFor(member: DiscoveryNode): Long {
        val value = indexFor(member) - 1
        backing.put(member, value)
        return value
    }

    fun put(member: DiscoveryNode, value: Long) {
        backing.put(member, value)
    }

    fun putIfGreater(member: DiscoveryNode, value: Long): Long {
        if (backing.containsKey(member)) {
            val prev = backing.get(member)
            if (prev < value) {
                backing.put(member, value)
                return value
            } else {
                return prev
            }
        } else {
            backing.put(member, value)
            return value
        }
    }

    fun consensusForIndex(config: ClusterConfiguration): Long {
        return when(config) {
            is JointConsensusClusterConfiguration -> {
                val oldQuorum = indexOnMajority(config.oldMembers)
                val newQuorum = indexOnMajority(config.newMembers)
                Math.min(oldQuorum, newQuorum)
            }
            else -> {
                indexOnMajority(config.members)
            }
        }
    }

    private fun indexOnMajority(include: Set<DiscoveryNode>): Long {
        if (include.isEmpty) {
            return 0
        }
        indexes.resetQuick()
        include.forEach { node -> indexes.add(indexFor(node)) }
        indexes.sort()
        val index = ceiling(include.size(), 2) - 1
        return indexes.get(index)
    }

    private fun ceiling(numerator: Int, divisor: Int): Int {
        return if (numerator % divisor == 0) {
            numerator / divisor
        } else {
            numerator / divisor + 1
        }
    }

    fun indexFor(member: DiscoveryNode): Long {
        return backing.get(member)
    }
}