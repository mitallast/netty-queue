package org.mitallast.queue.raft.cluster;

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.Version;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.raft.state.MemberState;
import org.mitallast.queue.transport.DiscoveryNode;
import org.mitallast.queue.transport.TransportService;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.when;

public class ClusterServiceTest extends BaseTest {

    @Mock
    private TransportService transportService;
    @Mock
    private Settings settings;

    private ClusterService clusterService;

    private DiscoveryNode localNode;
    private MemberState localState;
    private DiscoveryNode remoteNode;

    @Before
    public void setUp() throws Exception {
        localNode = new DiscoveryNode("test1", HostAndPort.fromParts("localhost", 123), Version.CURRENT);
        localState = new MemberState(localNode, MemberState.Type.ACTIVE);
        remoteNode = new DiscoveryNode("test2", HostAndPort.fromParts("localhost", 234), Version.CURRENT);

        MockitoAnnotations.initMocks(this);
        when(transportService.localNode()).thenReturn(localNode);

        clusterService = new ClusterService(settings, transportService);
        clusterService.start();
    }

    @After
    public void tearDown() throws Exception {
        clusterService.stop();
        clusterService.close();
    }

    @Test
    public void testEmpty() throws Exception {
        Assert.assertEquals(ImmutableList.of(localState), clusterService.members());
        Assert.assertEquals(ImmutableList.of(localNode), clusterService.nodes());
        Assert.assertEquals(ImmutableList.of(localState), clusterService.activeMembers());
        Assert.assertEquals(ImmutableList.of(localNode), clusterService.activeNodes());
        Assert.assertTrue(clusterService.passiveMembers().isEmpty());
        Assert.assertTrue(clusterService.passiveNodes().isEmpty());
    }

    @Test
    public void testAddActiveMember() throws Exception {
        MemberState remoteState = new MemberState(remoteNode, MemberState.Type.ACTIVE);
        clusterService.addMember(remoteState);

        Assert.assertEquals(ImmutableList.of(localState, remoteState), clusterService.members());
        Assert.assertEquals(ImmutableList.of(localNode, remoteNode), clusterService.nodes());
        Assert.assertEquals(ImmutableList.of(localState, remoteState), clusterService.activeMembers());
        Assert.assertEquals(ImmutableList.of(localNode, remoteNode), clusterService.activeNodes());
        Assert.assertTrue(clusterService.passiveMembers().isEmpty());
        Assert.assertTrue(clusterService.passiveNodes().isEmpty());
    }

    @Test
    public void testAddPassiveMember() throws Exception {
        MemberState remoteState = new MemberState(remoteNode, MemberState.Type.PASSIVE);
        clusterService.addMember(remoteState);

        Assert.assertEquals(ImmutableList.of(localState, remoteState), clusterService.members());
        Assert.assertEquals(ImmutableList.of(localNode, remoteNode), clusterService.nodes());
        Assert.assertEquals(ImmutableList.of(localState), clusterService.activeMembers());
        Assert.assertEquals(ImmutableList.of(localNode), clusterService.activeNodes());
        Assert.assertEquals(ImmutableList.of(remoteState), clusterService.passiveMembers());
        Assert.assertEquals(ImmutableList.of(remoteNode), clusterService.passiveNodes());
    }
}
