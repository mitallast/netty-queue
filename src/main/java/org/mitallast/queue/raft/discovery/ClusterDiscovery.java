package org.mitallast.queue.raft.discovery;

import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.transport.DiscoveryNode;
import org.mitallast.queue.transport.TransportServer;

import java.util.HashSet;
import java.util.Set;

public class ClusterDiscovery extends AbstractComponent {
    private final DiscoveryNode self;
    private final ImmutableSet<DiscoveryNode> discoveryNodes;

    @Inject
    public ClusterDiscovery(Config config) {
        super(config.getConfig("raft.discovery"), ClusterDiscovery.class);
        self = new DiscoveryNode(this.config.getString("host"), this.config.getInt("port"));
        discoveryNodes = parseDiscovery();
    }

    public DiscoveryNode self() {
        return self;
    }

    public ImmutableSet<DiscoveryNode> discoveryNodes() {
        return discoveryNodes;
    }

    private ImmutableSet<DiscoveryNode> parseDiscovery() {
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
