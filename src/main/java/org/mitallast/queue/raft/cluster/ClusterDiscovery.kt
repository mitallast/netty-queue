package org.mitallast.queue.raft.cluster

import com.google.common.net.HostAndPort
import com.google.inject.Inject
import com.typesafe.config.Config
import io.vavr.collection.HashSet
import io.vavr.collection.Set
import org.mitallast.queue.transport.DiscoveryNode
import org.mitallast.queue.transport.TransportServer

class ClusterDiscovery @Inject constructor(config: Config) {
    private val conf = config.getConfig("raft.discovery")
    val self = DiscoveryNode(conf.getString("host"), conf.getInt("port"))
    val discoveryNodes: Set<DiscoveryNode> = parseDiscovery(conf)

    private fun parseDiscovery(config: Config): Set<DiscoveryNode> {
        var nodes: Set<DiscoveryNode> = HashSet.of(self)

        if (config.hasPath("nodes")) {
            for (hosts in config.getStringList("nodes")) {
                hosts.split(",".toRegex()).dropLastWhile { it.isEmpty() }
                        .asSequence()
                        .map { token -> token.trim { it <= ' ' } }
                        .filterNot { it.isEmpty() }
                        .map { HostAndPort.fromString(it) }
                        .forEach {
                            nodes = nodes.add(DiscoveryNode(
                                    it.host,
                                    it.getPortOrDefault(TransportServer.DEFAULT_PORT)
                            ))
                        }
            }
        }
        return nodes
    }
}
