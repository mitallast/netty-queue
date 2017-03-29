package org.mitallast.queue.raft.discovery;

import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mitallast.queue.transport.DiscoveryNode;
import org.mitallast.queue.transport.TransportServer;

import java.util.HashSet;
import java.util.Set;

public class ClusterDiscovery {
    private final static Logger logger = LogManager.getLogger();
    private final DiscoveryNode self;
    private final ImmutableSet<DiscoveryNode> discoveryNodes;

    @Inject
    public ClusterDiscovery(Config config) {
        Config conf = config.getConfig("raft.discovery");
        self = new DiscoveryNode(conf.getString("host"), conf.getInt("port"));
        discoveryNodes = parseDiscovery(conf);
    }

    public DiscoveryNode self() {
        return self;
    }

    public ImmutableSet<DiscoveryNode> discoveryNodes() {
        return discoveryNodes;
    }

    private ImmutableSet<DiscoveryNode> parseDiscovery(Config config) {
        Set<DiscoveryNode> nodes = new HashSet<>();
        nodes.add(self);

        if (config.hasPath("nodes")) {
            for (String hosts : config.getStringList("nodes")) {
                for (String host : hosts.split(",")) {
                    host = host.trim();
                    if (!host.isEmpty()) {
                        HostAndPort hostAndPort = HostAndPort.fromString(host);
                        nodes.add(new DiscoveryNode(
                            hostAndPort.getHostText(),
                            hostAndPort.getPortOrDefault(TransportServer.DEFAULT_PORT)
                        ));
                    }
                }
            }
        }

        logger.info("nodes: {}", nodes);
        return ImmutableSet.copyOf(nodes);
    }
}
