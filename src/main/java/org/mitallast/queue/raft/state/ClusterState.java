package org.mitallast.queue.raft.state;

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import org.mitallast.queue.Version;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class ClusterState extends AbstractLifecycleComponent implements Iterable<MemberState> {
    private final Map<DiscoveryNode, MemberState> members = new HashMap<>();
    private final List<MemberState> activeMembers = new ArrayList<>();
    private final List<MemberState> passiveMembers = new ArrayList<>();

    @Inject
    public ClusterState(Settings settings) {
        super(settings);
    }

    public ClusterState addMember(MemberState member) {
        assert members.putIfAbsent(member.getNode(), member) == null;
        if (member.getType() == MemberState.Type.ACTIVE) {
            addActiveMember(member);
        } else {
            addPassiveMember(member);
        }
        return this;
    }

    private void addActiveMember(MemberState member) {
        activeMembers.add(member);
        logger.info("add active member {}", member.getNode());
        logger.info("active members: {}", activeMembers);
    }

    private void addPassiveMember(MemberState member) {
        passiveMembers.add(member);
    }

    ClusterState removeMember(MemberState member) {
        members.remove(member.getNode());
        if (member.getType() == MemberState.Type.ACTIVE) {
            removeActiveMember(member);
        } else {
            removePassiveMember(member);
        }
        return this;
    }

    private void removeActiveMember(MemberState member) {
        Iterator<MemberState> iterator = activeMembers.iterator();
        while (iterator.hasNext()) {
            if (iterator.next().getNode().equals(member.getNode())) {
                iterator.remove();
            }
        }
    }

    private void removePassiveMember(MemberState member) {
        Iterator<MemberState> iterator = passiveMembers.iterator();
        while (iterator.hasNext()) {
            if (iterator.next().getNode().equals(member.getNode())) {
                iterator.remove();
            }
        }
    }

    public MemberState getMember(DiscoveryNode node) {
        return members.get(node);
    }

    public ImmutableList<MemberState> getActiveMembers() {
        return ImmutableList.copyOf(activeMembers);
    }

    public ImmutableList<MemberState> getPassiveMembers() {
        return ImmutableList.copyOf(passiveMembers);
    }

    public ImmutableList<MemberState> getMembers() {
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
        return new ClusterStateIterator(members.entrySet().iterator());
    }

    @Override
    protected void doStart() throws IOException {
        String[] nodes = settings.getAsArray("raft.cluster.nodes");
        for (String node : nodes) {
            HostAndPort hostAndPort = HostAndPort.fromString(node);
            DiscoveryNode discoveryNode = new DiscoveryNode(node, hostAndPort, Version.CURRENT);
            addMember(new MemberState(discoveryNode, MemberState.Type.ACTIVE));
        }
    }

    @Override
    protected void doStop() throws IOException {

    }

    @Override
    protected void doClose() throws IOException {

    }

    private class ClusterStateIterator implements Iterator<MemberState> {
        private final Iterator<Map.Entry<DiscoveryNode, MemberState>> iterator;
        private MemberState member;

        private ClusterStateIterator(Iterator<Map.Entry<DiscoveryNode, MemberState>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public MemberState next() {
            member = iterator.next().getValue();
            return member;
        }

        @Override
        public void remove() {
            iterator.remove();
            removeMember(member);
        }
    }
}
