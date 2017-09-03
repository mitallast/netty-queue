package org.mitallast.queue.raft;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import javaslang.collection.Vector;
import javaslang.control.Option;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.raft.cluster.StableClusterConfiguration;
import org.mitallast.queue.raft.protocol.*;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.DataInput;
import java.io.DataOutput;

import static org.junit.Assert.assertEquals;

public class RaftProtocolTest extends BaseTest {
    private ByteBuf buffer;
    private DataInput input;
    private DataOutput output;

    @Before
    public void setUp() throws Exception {
        buffer = Unpooled.buffer();
    }

    @After
    public void tearDown() throws Exception {
        buffer.release();
    }

    @Test
    public void testAddServerCodec() throws Exception {
        AddServer expected = new AddServer(randomNode());
        // write
        output = new ByteBufOutputStream(buffer);
        AddServer.Companion.getCodec().write(output, expected);
        // read
        input = new ByteBufInputStream(buffer);
        AddServer actual = AddServer.Companion.getCodec().read(input);
        assertEquals(expected, actual);
    }

    @Test
    public void testAddServerResponseCodec() throws Exception {
        AddServerResponse expected = new AddServerResponse(
            AddServerResponse.Status.OK,
            Option.some(randomNode())
        );
        // write
        output = new ByteBufOutputStream(buffer);
        AddServerResponse.Companion.getCodec().write(output, expected);
        // read
        input = new ByteBufInputStream(buffer);
        AddServerResponse actual = AddServerResponse.Companion.getCodec().read(input);
        assertEquals(expected, actual);
    }

    @Test
    public void testAppendEntriesCodec() throws Exception {
        AppendEntries expected = new AppendEntries(
            randomNode(),
            random.nextLong(),
            random.nextLong(),
            random.nextLong(),
            random.nextLong(),
            Vector.of(
                randomLogEntry(),
                randomLogEntry()
            )
        );
        // write
        output = new ByteBufOutputStream(buffer);
        AppendEntries.Companion.getCodec().write(output, expected);
        // read
        input = new ByteBufInputStream(buffer);
        AppendEntries actual = AppendEntries.Companion.getCodec().read(input);
        assertEquals(expected, actual);
    }

    @Test
    public void testAppendRejectedCodec() throws Exception {
        AppendRejected expected = new AppendRejected(
            randomNode(),
            random.nextLong(),
            random.nextLong()
        );
        // write
        output = new ByteBufOutputStream(buffer);
        AppendRejected.Companion.getCodec().write(output, expected);
        // read
        input = new ByteBufInputStream(buffer);
        AppendRejected actual = AppendRejected.Companion.getCodec().read(input);
        assertEquals(expected, actual);
    }

    @Test
    public void testAppendSuccessfulCodec() throws Exception {
        AppendSuccessful expected = new AppendSuccessful(
            randomNode(),
            random.nextLong(),
            random.nextLong()
        );
        // write
        output = new ByteBufOutputStream(buffer);
        AppendSuccessful.Companion.getCodec().write(output, expected);
        // read
        input = new ByteBufInputStream(buffer);
        AppendSuccessful actual = AppendSuccessful.Companion.getCodec().read(input);
        assertEquals(expected, actual);
    }

    @Test
    public void testClientMessageCodec() throws Exception {
        ClientMessage expected = new ClientMessage(
                Noop.Companion.getINSTANCE(),
            random.nextLong()
        );
        // write
        output = new ByteBufOutputStream(buffer);
        ClientMessage.Companion.getCodec().write(output, expected);
        // read
        input = new ByteBufInputStream(buffer);
        ClientMessage actual = ClientMessage.Companion.getCodec().read(input);
        assertEquals(expected, actual);
    }

    @Test
    public void testDeclineCandidateCodec() throws Exception {
        DeclineCandidate expected = new DeclineCandidate(
            randomNode(),
            random.nextLong()
        );
        // write
        output = new ByteBufOutputStream(buffer);
        DeclineCandidate.Companion.getCodec().write(output, expected);
        // read
        input = new ByteBufInputStream(buffer);
        DeclineCandidate actual = DeclineCandidate.Companion.getCodec().read(input);
        assertEquals(expected, actual);
    }

    @Test
    public void testInstallSnapshotCodec() throws Exception {
        InstallSnapshot expected = new InstallSnapshot(
            randomNode(),
            random.nextLong(),
            new RaftSnapshot(
                new RaftSnapshotMetadata(
                    random.nextLong(),
                    random.nextLong(),
                    new StableClusterConfiguration(randomNode(), randomNode())
                ),
                Vector.of(Noop.Companion.getINSTANCE())
            )
        );
        // write
        output = new ByteBufOutputStream(buffer);
        InstallSnapshot.Companion.getCodec().write(output, expected);
        // read
        input = new ByteBufInputStream(buffer);
        InstallSnapshot actual = InstallSnapshot.Companion.getCodec().read(input);
        assertEquals(expected, actual);
    }

    @Test
    public void testInstallSnapshotRejectedCodec() throws Exception {
        InstallSnapshotRejected expected = new InstallSnapshotRejected(
            randomNode(),
            random.nextLong()
        );
        // write
        output = new ByteBufOutputStream(buffer);
        InstallSnapshotRejected.Companion.getCodec().write(output, expected);
        // read
        input = new ByteBufInputStream(buffer);
        InstallSnapshotRejected actual = InstallSnapshotRejected.Companion.getCodec().read(input);
        assertEquals(expected, actual);
    }

    @Test
    public void testInstallSnapshotSuccessfulCodec() throws Exception {
        InstallSnapshotSuccessful expected = new InstallSnapshotSuccessful(
            randomNode(),
            random.nextLong(),
            random.nextLong()
        );
        // write
        output = new ByteBufOutputStream(buffer);
        InstallSnapshotSuccessful.Companion.getCodec().write(output, expected);
        // read
        input = new ByteBufInputStream(buffer);
        InstallSnapshotSuccessful actual = InstallSnapshotSuccessful.Companion.getCodec().read(input);
        assertEquals(expected, actual);
    }

    @Test
    public void testLogEntryCodec() throws Exception {
        LogEntry expected = randomLogEntry();
        // write
        output = new ByteBufOutputStream(buffer);
        LogEntry.Companion.getCodec().write(output, expected);
        // read
        input = new ByteBufInputStream(buffer);
        LogEntry actual = LogEntry.Companion.getCodec().read(input);
        assertEquals(expected, actual);
    }

    @Test
    public void testNoopCodec() throws Exception {
        // write
        output = new ByteBufOutputStream(buffer);
        Noop.Companion.getCodec().write(output, Noop.Companion.getINSTANCE());
        // read
        input = new ByteBufInputStream(buffer);
        Noop actual = Noop.Companion.getCodec().read(input);
        assertEquals(Noop.Companion.getINSTANCE(), actual);
    }

    @Test
    public void testRaftSnapshotCodec() throws Exception {
        RaftSnapshot expected = new RaftSnapshot(
            new RaftSnapshotMetadata(
                random.nextLong(),
                random.nextLong(),
                new StableClusterConfiguration(randomNode(), randomNode())
            ),
            Vector.of(Noop.Companion.getINSTANCE())
        );
        // write
        output = new ByteBufOutputStream(buffer);
        RaftSnapshot.Companion.getCodec().write(output, expected);
        // read
        input = new ByteBufInputStream(buffer);
        RaftSnapshot actual = RaftSnapshot.Companion.getCodec().read(input);
        assertEquals(expected, actual);
    }

    @Test
    public void testRaftSnapshotMetadataCodec() throws Exception {
        RaftSnapshotMetadata expected = new RaftSnapshotMetadata(
            random.nextLong(),
            random.nextLong(),
            new StableClusterConfiguration(randomNode(), randomNode())
        );
        // write
        output = new ByteBufOutputStream(buffer);
        RaftSnapshotMetadata.Companion.getCodec().write(output, expected);
        // read
        input = new ByteBufInputStream(buffer);
        RaftSnapshotMetadata actual = RaftSnapshotMetadata.Companion.getCodec().read(input);
        assertEquals(expected, actual);
    }

    @Test
    public void testRemoveServerCodec() throws Exception {
        RemoveServer expected = new RemoveServer(randomNode());
        // write
        output = new ByteBufOutputStream(buffer);
        RemoveServer.Companion.getCodec().write(output, expected);
        // read
        input = new ByteBufInputStream(buffer);
        RemoveServer actual = RemoveServer.Companion.getCodec().read(input);
        assertEquals(expected, actual);
    }

    @Test
    public void testRemoveServerResponseCodec() throws Exception {
        RemoveServerResponse expected = new RemoveServerResponse(
            RemoveServerResponse.Status.OK,
            Option.some(randomNode())
        );
        // write
        output = new ByteBufOutputStream(buffer);
        RemoveServerResponse.Companion.getCodec().write(output, expected);
        // read
        input = new ByteBufInputStream(buffer);
        RemoveServerResponse actual = RemoveServerResponse.Companion.getCodec().read(input);
        assertEquals(expected, actual);
    }

    @Test
    public void testRequestVoteCodec() throws Exception {
        RequestVote expected = new RequestVote(
            random.nextLong(),
            randomNode(),
            random.nextLong(),
            random.nextLong()
        );
        // write
        output = new ByteBufOutputStream(buffer);
        RequestVote.Companion.getCodec().write(output, expected);
        // read
        input = new ByteBufInputStream(buffer);
        RequestVote actual = RequestVote.Companion.getCodec().read(input);
        assertEquals(expected, actual);
    }

    @Test
    public void testVoteCandidateCodec() throws Exception {
        VoteCandidate expected = new VoteCandidate(
            randomNode(),
            random.nextLong()
        );
        // write
        output = new ByteBufOutputStream(buffer);
        VoteCandidate.Companion.getCodec().write(output, expected);
        // read
        input = new ByteBufInputStream(buffer);
        VoteCandidate actual = VoteCandidate.Companion.getCodec().read(input);
        assertEquals(expected, actual);
    }

    private DiscoveryNode randomNode() {
        return new DiscoveryNode(randomString(), random.nextInt());
    }

    private LogEntry randomLogEntry() {
        return new LogEntry(
            random.nextLong(),
            random.nextLong(),
            random.nextLong(),
                Noop.Companion.getINSTANCE()
        );
    }
}
