package org.mitallast.queue.raft;

import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.TObjectLongMap;
import gnu.trove.map.hash.TObjectLongHashMap;
import javaslang.collection.Set;
import org.mitallast.queue.raft.cluster.ClusterConfiguration;
import org.mitallast.queue.raft.cluster.JointConsensusClusterConfiguration;
import org.mitallast.queue.transport.DiscoveryNode;

public class LogIndexMap {
    private final TObjectLongMap<DiscoveryNode> backing;
    private final TLongArrayList indexes = new TLongArrayList(64);

    public LogIndexMap(long defaultIndex) {
        this.backing = new TObjectLongHashMap<>(64, 0.5f, defaultIndex);
    }

    public long decrementFor(DiscoveryNode member) {
        long value = indexFor(member) - 1;
        backing.put(member, value);
        return value;
    }

    public void put(DiscoveryNode member, long value) {
        backing.put(member, value);
    }

    public long putIfGreater(DiscoveryNode member, long value) {
        if (backing.containsKey(member)) {
            long prev = backing.get(member);
            if (prev < value) {
                backing.put(member, value);
                return value;
            } else {
                return prev;
            }
        } else {
            backing.put(member, value);
            return value;
        }
    }

    public long consensusForIndex(ClusterConfiguration config) {
        if (config.isTransitioning()) { // joint
            long oldQuorum = indexOnMajority(((JointConsensusClusterConfiguration) config).getOldMembers());
            long newQuorum = indexOnMajority(((JointConsensusClusterConfiguration) config).getNewMembers());
            return Math.min(oldQuorum, newQuorum);
        } else { // stable
            return indexOnMajority(config.members());
        }
    }

    private long indexOnMajority(Set<DiscoveryNode> include) {
        if (include.isEmpty()) {
            return 0;
        }
        indexes.resetQuick();
        include.forEach(node -> indexes.add(indexFor(node)));
        indexes.sort();
        int index = ceiling(include.size(), 2) - 1;
        return indexes.get(index);
    }

    private int ceiling(int numerator, int divisor) {
        if (numerator % divisor == 0) {
            return numerator / divisor;
        } else {
            return (numerator / divisor) + 1;
        }
    }

    public long indexFor(DiscoveryNode member) {
        return backing.get(member);
    }
}
