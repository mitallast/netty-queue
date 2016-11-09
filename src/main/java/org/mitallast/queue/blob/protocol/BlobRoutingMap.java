package org.mitallast.queue.blob.protocol;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableMap;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;
import java.util.Map;

public class BlobRoutingMap implements Streamable {

    private final ImmutableMap<String, ImmutableSet<DiscoveryNode>> routingMap;

    public BlobRoutingMap(ImmutableMap<String, ImmutableSet<DiscoveryNode>> routingMap) {
        this.routingMap = routingMap;
    }

    public BlobRoutingMap(StreamInput stream) throws IOException {
        ImmutableMap.Builder<String, ImmutableSet<DiscoveryNode>> map = ImmutableMap.builder();
        int keys = stream.readInt();
        for (int i = 0; i < keys; i++) {
            String key = stream.readText();
            map.put(key, stream.readStreamableSet(DiscoveryNode::new));
        }
        routingMap = map.build();
    }

    public ImmutableMap<String, ImmutableSet<DiscoveryNode>> getRoutingMap() {
        return routingMap;
    }

    public BlobRoutingMap withResource(String key, DiscoveryNode node) {
        if (routingMap.containsKey(key)) {
            ImmutableMap.Builder<String, ImmutableSet<DiscoveryNode>> map = ImmutableMap.builder();
            routingMap.entrySet().stream()
                .filter(entry -> !entry.getKey().equals(key))
                .forEach(map::put);
            ImmutableSet<DiscoveryNode> nodes = ImmutableSet.<DiscoveryNode>builder()
                .addAll(routingMap.get(key))
                .add(node)
                .build();
            map.put(key, nodes);
            return new BlobRoutingMap(map.build());
        } else {
            ImmutableMap.Builder<String, ImmutableSet<DiscoveryNode>> map = ImmutableMap.builder();
            map.putAll(routingMap);
            map.put(key, ImmutableSet.of(node));
            return new BlobRoutingMap(map.build());
        }
    }

    public BlobRoutingMap withResource(String key, ImmutableSet<DiscoveryNode> nodes) {
        ImmutableMap.Builder<String, ImmutableSet<DiscoveryNode>> map = ImmutableMap.builder();
        map.putAll(routingMap);
        map.put(key, nodes);
        return new BlobRoutingMap(map.build());
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeInt(routingMap.size());
        for (Map.Entry<String, ImmutableSet<DiscoveryNode>> entry : routingMap.entrySet()) {
            stream.writeText(entry.getKey());
            stream.writeStreamableSet(entry.getValue());
        }
    }
}
