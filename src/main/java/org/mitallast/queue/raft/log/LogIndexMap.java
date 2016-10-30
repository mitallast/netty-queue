package org.mitallast.queue.raft.log;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.mitallast.queue.common.Immutable;
import org.mitallast.queue.raft.cluster.ClusterConfiguration;
import org.mitallast.queue.raft.cluster.JointConsensusClusterConfiguration;
import org.mitallast.queue.transport.DiscoveryNode;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class LogIndexMap {

    private ImmutableMap<DiscoveryNode, Long> backing;

    public LogIndexMap() {
        this(ImmutableMap.of());
    }

    public LogIndexMap(ImmutableMap<DiscoveryNode, Long> backing) {
        this.backing = backing;
    }

    public long decrementFor(DiscoveryNode member) {
        Preconditions.checkArgument(backing.containsKey(member), "Member [" + member + "] not found");
        long value = backing.get(member) - 1;
        backing = Immutable.replace(backing, member, value);
        return value;
    }

    public long incrementFor(DiscoveryNode member) {
        Preconditions.checkArgument(backing.containsKey(member), "Member [" + member + "] not found");
        long value = backing.get(member) + 1;
        backing = Immutable.replace(backing, member, value);
        return value;
    }

    public void put(DiscoveryNode member, long value) {
        backing = Immutable.replace(backing, member, value);
    }

    public long putIfGreater(DiscoveryNode member, long value) {
        if (backing.containsKey(member)) {
            long prev = backing.get(member);
            if (prev < value) {
                backing = Immutable.replace(backing, member, value);
                return value;
            } else {
                return prev;
            }
        } else {
            backing = Immutable.replace(backing, member, value);
            return value;
        }
    }

    public Optional<Long> consensusForIndex(ClusterConfiguration config) {
        if (config.isTransitioning()) { // joint
            Optional<Long> oldQuorum = indexOnMajority(((JointConsensusClusterConfiguration) config).getOldMembers());
            Optional<Long> newQuorum = indexOnMajority(((JointConsensusClusterConfiguration) config).getNewMembers());

            if (!oldQuorum.isPresent()) {
                return newQuorum;
            } else if (!newQuorum.isPresent()) {
                return oldQuorum;
            } else {
                return Optional.of(Math.min(oldQuorum.get(), newQuorum.get()));
            }
        } else { // stable
            return indexOnMajority(config.members());
        }
    }

    private Optional<Long> indexOnMajority(ImmutableSet<DiscoveryNode> include) {
        if (include.isEmpty()) {
            return Optional.empty();
        } else {
            int index = ceiling(include.size(), 2) - 1;
            List<Long> sorted = backing.entrySet()
                    .stream()
                    .filter(entry -> include.contains(entry.getKey()))
                    .map(Map.Entry::getValue)
                    .sorted()
                    .collect(Collectors.toList());
            if (sorted.size() <= index) {
                return Optional.empty();
            } else {
                return Optional.of(sorted.get(index));
            }
        }
    }

    private int ceiling(int numerator, int divisor) {
        if (numerator % divisor == 0) {
            return numerator / divisor;
        } else {
            return (numerator / divisor) + 1;
        }
    }

    public Optional<Long> indexFor(DiscoveryNode member) {
        return Optional.ofNullable(backing.get(member));
    }
}
