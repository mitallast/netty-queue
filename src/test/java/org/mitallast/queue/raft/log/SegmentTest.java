package org.mitallast.queue.raft.log;

import com.google.common.net.HostAndPort;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.Version;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.common.settings.ImmutableSettings;
import org.mitallast.queue.common.stream.InternalStreamService;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.StreamService;
import org.mitallast.queue.common.unit.ByteSizeUnit;
import org.mitallast.queue.raft.RaftStreamService;
import org.mitallast.queue.raft.log.entry.*;
import org.mitallast.queue.raft.resource.manager.CreatePath;
import org.mitallast.queue.raft.resource.manager.PathExists;
import org.mitallast.queue.raft.util.ExecutionContext;
import org.mitallast.queue.transport.DiscoveryNode;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.unitils.reflectionassert.ReflectionAssert;

import java.io.File;

public class SegmentTest extends BaseTest {
    @Spy
    private ExecutionContext executionContext;
    private StreamService streamService;
    private SegmentDescriptor descriptor;
    private Segment segment;
    private SegmentIndex segmentIndex;

    private LogEntryGenerator entryGenerator = new LogEntryGenerator(random);

    @Before
    public void setUp() throws Exception {
        streamService = new InternalStreamService(ImmutableSettings.EMPTY);
        new RaftStreamService(streamService);
        executionContext = new ExecutionContext(ImmutableSettings.EMPTY);
        executionContext.start();

        MockitoAnnotations.initMocks(this);
        Mockito.doNothing().when(executionContext).checkThread();

        descriptor = SegmentDescriptor.builder()
            .setId(0)
            .setIndex(0)
            .setMaxEntries(100)
            .setMaxEntrySize(1000)
            .setMaxSegmentSize(ByteSizeUnit.MB.toBytes(10))
            .setVersion(0).build();

        File file = testFolder.newFile();
        try (StreamOutput output = streamService.output(file)) {
            output.writeStreamable(descriptor.toBuilder());
        }

        segmentIndex = new SegmentIndex(testFolder.newFile(), (int) descriptor.maxEntries());
        segment = new Segment(streamService, file, segmentIndex, executionContext);
    }

    @After
    public void tearDown() throws Exception {
        segment.close();
        segmentIndex.close();
        executionContext.stop();
        executionContext.close();
    }

    @Test
    public void testAppendEntry() throws Exception {
        LogEntry[] entries = generate((int) segment.descriptor().maxEntries());
        for (LogEntry entry : entries) {
            segment.appendEntry(entry);
        }

        for (LogEntry entry : entries) {
            LogEntry actual = segment.getEntry(entry.index());
            Assert.assertNotNull(actual);
            ReflectionAssert.assertReflectionEquals(actual, entry);
        }
    }

    @Test
    public void testReopen() throws Exception {
        LogEntry[] entries = generate((int) segment.descriptor().maxEntries());
        for (LogEntry entry : entries) {
            segment.appendEntry(entry);
        }

        segment.flush();

        try (SegmentIndex reopenSegmentIndex = new SegmentIndex(segmentIndex.file(), (int) descriptor.maxEntries());
             Segment reopenSegment = new Segment(streamService, segment.file(), reopenSegmentIndex, executionContext)
        ) {
            ReflectionAssert.assertReflectionEquals(descriptor, reopenSegment.descriptor());
            Assert.assertFalse(reopenSegment.isEmpty());
            Assert.assertTrue(reopenSegment.isFull());
            Assert.assertEquals(segment.size(), reopenSegment.size());
            Assert.assertEquals(segment.length(), reopenSegment.length());
            Assert.assertEquals(segment.firstIndex(), reopenSegment.firstIndex());
            Assert.assertEquals(segment.lastIndex(), reopenSegment.lastIndex());
            Assert.assertEquals(segment.nextIndex(), reopenSegment.nextIndex());

            for (LogEntry entry : entries) {
                Assert.assertTrue(segment.containsIndex(entry.index()));
                Assert.assertTrue(segment.containsEntry(entry.index()));
                ReflectionAssert.assertReflectionEquals(entry, segment.getEntry(entry.index()));
            }
        }
    }

    private LogEntry[] generate(int max) {
        LogEntry[] entries = new LogEntry[max];
        for (int i = 0; i < max; i++) {

            switch (random.nextInt(7)) {
                case 0:
                    entries[i] = CommandEntry.builder()
                        .setIndex(i)
                        .setTerm(i)
                        .setTimestamp(random.nextLong())
                        .setSession(random.nextLong())
                        .setRequest(random.nextLong())
                        .setResponse(random.nextLong())
                        .setCommand(new CreatePath("some path"))
                        .build();
                    break;
                case 1:
                    entries[i] = JoinEntry.builder()
                        .setIndex(i)
                        .setTerm(i)
                        .setMember(randomNode())
                        .build();
                    break;
                case 2:
                    entries[i] = KeepAliveEntry.builder()
                        .setIndex(i)
                        .setTerm(i)
                        .setTimestamp(random.nextLong())
                        .setSession(random.nextLong())
                        .build();
                    break;
                case 3:
                    entries[i] = LeaveEntry.builder()
                        .setIndex(i)
                        .setTerm(i)
                        .setMember(randomNode())
                        .build();
                    break;
                case 4:
                    entries[i] = NoOpEntry.builder()
                        .setIndex(i)
                        .setTerm(i)
                        .build();
                    break;
                case 5:
                    entries[i] = QueryEntry.builder()
                        .setIndex(i)
                        .setTerm(i)
                        .setTimestamp(random.nextLong())
                        .setVersion(random.nextInt())
                        .setSession(random.nextInt())
                        .setQuery(new PathExists("some path"))
                        .build();
                    break;
                case 6:
                    entries[i] = RegisterEntry.builder()
                        .setIndex(i)
                        .setTerm(i)
                        .setTimestamp(random.nextLong())
                        .build();
                    break;
                default:
                    assert false;
            }
        }
        return entries;
    }

    private DiscoveryNode randomNode() {
        return new DiscoveryNode(randomUUID().toString(), HostAndPort.fromParts("localhost", random.nextInt(10000)), Version.CURRENT);
    }
}
