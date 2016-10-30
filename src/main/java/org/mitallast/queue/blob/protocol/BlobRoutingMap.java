package org.mitallast.queue.blob.protocol;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;
import java.util.Map;

public class BlobRoutingMap implements Streamable {

    private final ImmutableMap<String, ImmutableList<DiscoveryNode>> routingMap;

    public BlobRoutingMap(ImmutableMap<String, ImmutableList<DiscoveryNode>> routingMap) {
        this.routingMap = routingMap;
    }

    public BlobRoutingMap(StreamInput stream) throws IOException {
        ImmutableMap.Builder<String, ImmutableList<DiscoveryNode>> map = ImmutableMap.builder();
        int keys = stream.readInt();
        for (int i = 0; i < keys; i++) {
            String key = stream.readText();
            map.put(key, stream.readStreamableList(DiscoveryNode::new));
        }
        routingMap = map.build();
    }

    public ImmutableMap<String, ImmutableList<DiscoveryNode>> getRoutingMap() {
        return routingMap;
    }

    public BlobRoutingMap withResource(String key, DiscoveryNode node) {
        if (routingMap.containsKey(key)) {
            ImmutableMap.Builder<String, ImmutableList<DiscoveryNode>> map = ImmutableMap.builder();
            routingMap.entrySet().stream()
                .filter(entry -> !entry.getKey().equals(key))
                .forEach(map::put);
            ImmutableList<DiscoveryNode> nodes = ImmutableList.<DiscoveryNode>builder()
                .addAll(routingMap.get(key))
                .add(node)
                .build();
            map.put(key, nodes);
            return new BlobRoutingMap(map.build());
        } else {
            ImmutableMap.Builder<String, ImmutableList<DiscoveryNode>> map = ImmutableMap.builder();
            map.putAll(routingMap);
            map.put(key, ImmutableList.of(node));
            return new BlobRoutingMap(map.build());
        }
    }

    public BlobRoutingMap withResource(String key, ImmutableList<DiscoveryNode> nodes) {
        ImmutableMap.Builder<String, ImmutableList<DiscoveryNode>> map = ImmutableMap.builder();
        map.putAll(routingMap);
        map.put(key, nodes);
        return new BlobRoutingMap(map.build());
    }

    @Override
    public void writeTo(StreamOutput stream) throws IOException {
        stream.writeInt(routingMap.size());
        for (Map.Entry<String, ImmutableList<DiscoveryNode>> entry : routingMap.entrySet()) {
            stream.writeText(entry.getKey());
            stream.writeStreamableList(entry.getValue());
        }
    }
}
