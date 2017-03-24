package org.mitallast.queue.raft;

import gnu.trove.map.TObjectLongMap;
import gnu.trove.map.hash.TObjectLongHashMap;
import org.mitallast.queue.proto.raft.DiscoveryNode;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class LogIndexMap {
    private final TObjectLongMap<DiscoveryNode> backing;

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

    public long consensusForIndex(RaftMetadata meta) {
        if (meta.isTransitioning()) { // joint
            long oldQuorum = indexOnMajority(meta.getConfig().getJoint().getOldMembersList());
            long newQuorum = indexOnMajority(meta.getConfig().getJoint().getNewMembersList());
            return Math.min(oldQuorum, newQuorum);
        } else { // stable
            return indexOnMajority(meta.getConfig().getStable().getMembersList());
        }
    }

    private long indexOnMajority(Collection<DiscoveryNode> include) {
        if (include.isEmpty()) {
            return 0;
        }
        int index = ceiling(include.size()) - 1;
        List<Long> sorted = include.stream()
            .map(this::indexFor)
            .sorted()
            .collect(Collectors.toList());
        return sorted.get(index);
    }

    private int ceiling(int numerator) {
        if (numerator % 2 == 0) {
            return numerator / 2;
        } else {
            return (numerator / 2) + 1;
        }
    }

    public long indexFor(DiscoveryNode member) {
        return backing.get(member);
    }
}
