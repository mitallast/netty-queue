package org.mitallast.queue.raft.state;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.raft.cluster.Member;
import org.mitallast.queue.raft.util.ExecutionContext;
import org.mitallast.queue.transport.DiscoveryNode;

import java.util.*;
import java.util.stream.Collectors;

public class ClusterState extends AbstractComponent implements Iterable<MemberState> {
    private final Map<DiscoveryNode, MemberState> members = new HashMap<>();
    private final List<MemberState> activeMembers = new ArrayList<>();
    private final List<MemberState> passiveMembers = new ArrayList<>();
    private final ExecutionContext executionContext;

    @Inject
    public ClusterState(Settings settings, ExecutionContext executionContext) {
        super(settings);
        this.executionContext = executionContext;
    }

    public ClusterState addMember(MemberState member) {
        executionContext.checkThread();
        assert members.putIfAbsent(member.getNode(), member) == null;
        if (member.getType() == Member.Type.ACTIVE) {
            addActiveMember(member);
        } else {
            addPassiveMember(member);
        }
        return this;
    }

    private void addActiveMember(MemberState member) {
        executionContext.checkThread();
        activeMembers.add(member);
        logger.info("add active member {}", member.getNode());
        logger.info("active members: {}", activeMembers);
    }

    private void addPassiveMember(MemberState member) {
        executionContext.checkThread();
        passiveMembers.add(member);
    }

    ClusterState removeMember(MemberState member) {
        executionContext.checkThread();
        members.remove(member.getNode());
        if (member.getType() == Member.Type.ACTIVE) {
            removeActiveMember(member);
        } else {
            removePassiveMember(member);
        }
        return this;
    }

    private void removeActiveMember(MemberState member) {
        executionContext.checkThread();
        Iterator<MemberState> iterator = activeMembers.iterator();
        while (iterator.hasNext()) {
            if (iterator.next().getNode().equals(member.getNode())) {
                iterator.remove();
            }
        }
    }

    private void removePassiveMember(MemberState member) {
        executionContext.checkThread();
        Iterator<MemberState> iterator = passiveMembers.iterator();
        while (iterator.hasNext()) {
            if (iterator.next().getNode().equals(member.getNode())) {
                iterator.remove();
            }
        }
    }

    public MemberState getMember(DiscoveryNode node) {
        executionContext.checkThread();
        return members.get(node);
    }

    public ImmutableList<MemberState> getActiveMembers() {
        executionContext.checkThread();
        return ImmutableList.copyOf(activeMembers);
    }

    public ImmutableList<MemberState> getPassiveMembers() {
        executionContext.checkThread();
        return ImmutableList.copyOf(passiveMembers);
    }

    public ImmutableList<MemberState> getMembers() {
        executionContext.checkThread();
        return ImmutableList.copyOf(members.values());
    }

    public ImmutableList<DiscoveryNode> activeNodes() {
        return ImmutableList.copyOf(getActiveMembers().stream().map(MemberState::getNode).collect(Collectors.toList()));
    }

    public ImmutableList<DiscoveryNode> passiveNodes() {
        return ImmutableList.copyOf(getPassiveMembers().stream().map(MemberState::getNode).collect(Collectors.toList()));
    }

    public ImmutableList<DiscoveryNode> nodes() {
        return ImmutableList.copyOf(getMembers().stream().map(MemberState::getNode).collect(Collectors.toList()));
    }

    @Override
    public Iterator<MemberState> iterator() {
        executionContext.checkThread();
        return new ClusterStateIterator(members.entrySet().iterator());
    }

    private class ClusterStateIterator implements Iterator<MemberState> {
        private final Iterator<Map.Entry<DiscoveryNode, MemberState>> iterator;
        private MemberState member;

        private ClusterStateIterator(Iterator<Map.Entry<DiscoveryNode, MemberState>> iterator) {
            executionContext.checkThread();
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            executionContext.checkThread();
            return iterator.hasNext();
        }

        @Override
        public MemberState next() {
            executionContext.checkThread();
            member = iterator.next().getValue();
            return member;
        }

        @Override
        public void remove() {
            executionContext.checkThread();
            iterator.remove();
            removeMember(member);
        }
    }
}
