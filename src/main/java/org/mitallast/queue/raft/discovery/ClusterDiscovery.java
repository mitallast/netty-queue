package org.mitallast.queue.raft.discovery;

import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import javaslang.collection.HashSet;
import javaslang.collection.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mitallast.queue.transport.DiscoveryNode;
import org.mitallast.queue.transport.TransportServer;

public class ClusterDiscovery {
    private final static Logger logger = LogManager.getLogger();
    private final DiscoveryNode self;
    private final Set<DiscoveryNode> discoveryNodes;

    @Inject
    public ClusterDiscovery(Config config) {
        Config conf = config.getConfig("raft.discovery");
        self = new DiscoveryNode(conf.getString("host"), conf.getInt("port"));
        discoveryNodes = parseDiscovery(conf);
    }

    public DiscoveryNode self() {
        return self;
    }

    public Set<DiscoveryNode> discoveryNodes() {
        return discoveryNodes;
    }

    private Set<DiscoveryNode> parseDiscovery(Config config) {
        Set<DiscoveryNode> nodes = HashSet.of(self);

        if (config.hasPath("nodes")) {
            for (String hosts : config.getStringList("nodes")) {
                for (String host : hosts.split(",")) {
                    host = host.trim();
                    if (!host.isEmpty()) {
                        HostAndPort hostAndPort = HostAndPort.fromString(host);
                        nodes = nodes.add(new DiscoveryNode(
                            hostAndPort.getHostText(),
                            hostAndPort.getPortOrDefault(TransportServer.DEFAULT_PORT)
                        ));
                    }
                }
            }
        }

        logger.info("nodes: {}", nodes);
        return nodes;
    }
}
