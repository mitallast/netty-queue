package org.mitallast.queue.raft.discovery;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.transport.DiscoveryNode;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ClusterDiscovery extends AbstractComponent {
    private final ImmutableSet<DiscoveryNode> discoveryNodes;

    @Inject
    public ClusterDiscovery(Config config) {
        super(config.getConfig("raft.discovery"), ClusterDiscovery.class);
        discoveryNodes = parseDiscovery();
    }

    public ImmutableSet<DiscoveryNode> getDiscoveryNodes() {
        return discoveryNodes;
    }

    private ImmutableSet<DiscoveryNode> parseDiscovery() {
        Set<DiscoveryNode> nodes = new HashSet<>();

        List<Integer> portRange = config.getIntList("port-range");

        if (portRange.size() == 2) {
            String host = config.getString("host");
            int from = portRange.get(0);
            int to = portRange.get(1);
            logger.info("host {}, port range {}-{}", host, from, to);
            for (int port = from; port <= to; port++) {
                nodes.add(new DiscoveryNode(host, port));
            }
        }

        for (Config node : config.getConfigList("nodes")) {
            String host = node.getString("host");
            int port = node.getInt("port");
            nodes.add(new DiscoveryNode(host, port));
        }

        return ImmutableSet.copyOf(nodes);
    }
}
