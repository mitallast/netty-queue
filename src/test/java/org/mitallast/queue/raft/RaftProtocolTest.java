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
        AddServer.codec.write(output, expected);
        // read
        input = new ByteBufInputStream(buffer);
        AddServer actual = AddServer.codec.read(input);
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
        AddServerResponse.codec.write(output, expected);
        // read
        input = new ByteBufInputStream(buffer);
        AddServerResponse actual = AddServerResponse.codec.read(input);
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
        AppendEntries.codec.write(output, expected);
        // read
        input = new ByteBufInputStream(buffer);
        AppendEntries actual = AppendEntries.codec.read(input);
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
        AppendRejected.codec.write(output, expected);
        // read
        input = new ByteBufInputStream(buffer);
        AppendRejected actual = AppendRejected.codec.read(input);
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
        AppendSuccessful.codec.write(output, expected);
        // read
        input = new ByteBufInputStream(buffer);
        AppendSuccessful actual = AppendSuccessful.codec.read(input);
        assertEquals(expected, actual);
    }

    @Test
    public void testClientMessageCodec() throws Exception {
        ClientMessage expected = new ClientMessage(
            Noop.INSTANCE,
            random.nextLong()
        );
        // write
        output = new ByteBufOutputStream(buffer);
        ClientMessage.codec.write(output, expected);
        // read
        input = new ByteBufInputStream(buffer);
        ClientMessage actual = ClientMessage.codec.read(input);
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
        DeclineCandidate.codec.write(output, expected);
        // read
        input = new ByteBufInputStream(buffer);
        DeclineCandidate actual = DeclineCandidate.codec.read(input);
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
                Vector.of(Noop.INSTANCE)
            )
        );
        // write
        output = new ByteBufOutputStream(buffer);
        InstallSnapshot.codec.write(output, expected);
        // read
        input = new ByteBufInputStream(buffer);
        InstallSnapshot actual = InstallSnapshot.codec.read(input);
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
        InstallSnapshotRejected.codec.write(output, expected);
        // read
        input = new ByteBufInputStream(buffer);
        InstallSnapshotRejected actual = InstallSnapshotRejected.codec.read(input);
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
        InstallSnapshotSuccessful.codec.write(output, expected);
        // read
        input = new ByteBufInputStream(buffer);
        InstallSnapshotSuccessful actual = InstallSnapshotSuccessful.codec.read(input);
        assertEquals(expected, actual);
    }

    @Test
    public void testLogEntryCodec() throws Exception {
        LogEntry expected = randomLogEntry();
        // write
        output = new ByteBufOutputStream(buffer);
        LogEntry.codec.write(output, expected);
        // read
        input = new ByteBufInputStream(buffer);
        LogEntry actual = LogEntry.codec.read(input);
        assertEquals(expected, actual);
    }

    @Test
    public void testNoopCodec() throws Exception {
        // write
        output = new ByteBufOutputStream(buffer);
        Noop.codec.write(output, Noop.INSTANCE);
        // read
        input = new ByteBufInputStream(buffer);
        Noop actual = Noop.codec.read(input);
        assertEquals(Noop.INSTANCE, actual);
    }

    @Test
    public void testRaftSnapshotCodec() throws Exception {
        RaftSnapshot expected = new RaftSnapshot(
            new RaftSnapshotMetadata(
                random.nextLong(),
                random.nextLong(),
                new StableClusterConfiguration(randomNode(), randomNode())
            ),
            Vector.of(Noop.INSTANCE)
        );
        // write
        output = new ByteBufOutputStream(buffer);
        RaftSnapshot.codec.write(output, expected);
        // read
        input = new ByteBufInputStream(buffer);
        RaftSnapshot actual = RaftSnapshot.codec.read(input);
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
        RaftSnapshotMetadata.codec.write(output, expected);
        // read
        input = new ByteBufInputStream(buffer);
        RaftSnapshotMetadata actual = RaftSnapshotMetadata.codec.read(input);
        assertEquals(expected, actual);
    }

    @Test
    public void testRemoveServerCodec() throws Exception {
        RemoveServer expected = new RemoveServer(randomNode());
        // write
        output = new ByteBufOutputStream(buffer);
        RemoveServer.codec.write(output, expected);
        // read
        input = new ByteBufInputStream(buffer);
        RemoveServer actual = RemoveServer.codec.read(input);
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
        RemoveServerResponse.codec.write(output, expected);
        // read
        input = new ByteBufInputStream(buffer);
        RemoveServerResponse actual = RemoveServerResponse.codec.read(input);
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
        RequestVote.codec.write(output, expected);
        // read
        input = new ByteBufInputStream(buffer);
        RequestVote actual = RequestVote.codec.read(input);
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
        VoteCandidate.codec.write(output, expected);
        // read
        input = new ByteBufInputStream(buffer);
        VoteCandidate actual = VoteCandidate.codec.read(input);
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
            Noop.INSTANCE
        );
    }
}
