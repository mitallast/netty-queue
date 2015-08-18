package org.mitallast.queue.raft.cluster;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import org.mitallast.queue.Version;
import org.mitallast.queue.common.Immutable;
import org.mitallast.queue.common.component.AbstractLifecycleComponent;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.raft.state.MemberState;
import org.mitallast.queue.transport.DiscoveryNode;
import org.mitallast.queue.transport.TransportService;

import java.io.IOException;

public class ClusterService extends AbstractLifecycleComponent {
    private final TransportService transportService;

    private ImmutableMap<DiscoveryNode, MemberState> membersMap = ImmutableMap.of();
    private ImmutableList<MemberState> members = ImmutableList.of();
    private ImmutableList<DiscoveryNode> nodes = ImmutableList.of();

    private ImmutableList<MemberState> activeMembers = ImmutableList.of();
    private ImmutableList<DiscoveryNode> activeNodes = ImmutableList.of();

    private ImmutableList<MemberState> passiveMembers = ImmutableList.of();
    private ImmutableList<DiscoveryNode> passiveNodes = ImmutableList.of();

    @Inject
    public ClusterService(Settings settings, TransportService transportService) {
        super(settings);
        this.transportService = transportService;
    }

    public synchronized void addMember(MemberState member) {
        if (!membersMap.containsKey(member.getNode())) {
            membersMap = Immutable.compose(membersMap, member.getNode(), member);
            members = ImmutableList.copyOf(membersMap.values());
            nodes = ImmutableList.copyOf(members.stream().map(MemberState::getNode).iterator());

            if (member.getType() == MemberState.Type.ACTIVE) {
                logger.info("add active member {}", member.getNode());
                activeMembers = Immutable.compose(activeMembers, member);
                activeNodes = ImmutableList.copyOf(activeMembers.stream().map(MemberState::getNode).iterator());
                logger.info("active members: {}", activeNodes);
            } else {
                logger.info("add passive member {}", member.getNode());
                passiveMembers = Immutable.compose(passiveMembers, member);
                passiveNodes = ImmutableList.copyOf(passiveMembers.stream().map(MemberState::getNode).iterator());
                logger.info("passive members: {}", passiveNodes);
            }
        }
    }

    public synchronized void removeMember(MemberState member) {
        if (membersMap.containsKey(member.getNode())) {
            membersMap = Immutable.subtract(membersMap, member.getNode());
            members = ImmutableList.copyOf(membersMap.values());
            nodes = ImmutableList.copyOf(members.stream().map(MemberState::getNode).iterator());

            if (member.getType() == MemberState.Type.ACTIVE) {
                logger.info("remove active member {}", member.getNode());
                activeMembers = Immutable.subtract(activeMembers, member);
                activeNodes = ImmutableList.copyOf(activeMembers.stream().map(MemberState::getNode).iterator());
                logger.info("active members: {}", activeNodes);
            } else {
                logger.info("remove passive member {}", member.getNode());
                passiveMembers = Immutable.subtract(passiveMembers, member);
                passiveNodes = ImmutableList.copyOf(passiveMembers.stream().map(MemberState::getNode).iterator());
                logger.info("passive members: {}", passiveNodes);
            }
        }
    }

    public MemberState member(DiscoveryNode node) {
        return membersMap.get(node);
    }

    public ImmutableList<MemberState> activeMembers() {
        return activeMembers;
    }

    public ImmutableList<MemberState> passiveMembers() {
        return passiveMembers;
    }

    public ImmutableList<MemberState> members() {
        return members;
    }

    public ImmutableList<DiscoveryNode> activeNodes() {
        return activeNodes;
    }

    public ImmutableList<DiscoveryNode> passiveNodes() {
        return passiveNodes;
    }

    public ImmutableList<DiscoveryNode> nodes() {
        return nodes;
    }

    public boolean containsNode(DiscoveryNode node) {
        return membersMap.containsKey(node);
    }

    @Override
    protected void doStart() throws IOException {
        MemberState.Type type = settings.getAsBoolean("raft.passive", false) ? MemberState.Type.PASSIVE : MemberState.Type.ACTIVE;
        addMember(new MemberState(transportService.localNode(), type));
        String[] nodes = settings.getAsArray("raft.cluster.nodes");
        if (nodes != null && nodes.length > 0) {
            for (String node : nodes) {
                HostAndPort hostAndPort = HostAndPort.fromString(node);
                if (!hostAndPort.equals(transportService.localAddress())) {
                    DiscoveryNode discoveryNode = new DiscoveryNode(node, hostAndPort, Version.CURRENT);
                    addMember(new MemberState(discoveryNode, MemberState.Type.ACTIVE));
                }
            }
        }
    }

    @Override
    protected void doStop() throws IOException {

    }

    @Override
    protected void doClose() throws IOException {

    }
}
