package org.mitallast.queue.raft.log;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseTest;
import org.mitallast.queue.common.settings.ImmutableSettings;
import org.mitallast.queue.common.stream.InternalStreamService;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.StreamService;
import org.mitallast.queue.common.unit.ByteSizeUnit;
import org.mitallast.queue.raft.RaftStreamService;
import org.mitallast.queue.raft.log.entry.QueryEntry;
import org.mitallast.queue.raft.resource.manager.PathExists;
import org.mitallast.queue.raft.util.ExecutionContext;

import java.io.File;
import java.io.IOException;

public class SegmentTest extends BaseTest {

    private ExecutionContext executionContext;
    private Segment segment;

    @Before
    public void setUp() throws Exception {
        StreamService streamService = new InternalStreamService(ImmutableSettings.EMPTY);
        new RaftStreamService(streamService);
        executionContext = new ExecutionContext(ImmutableSettings.EMPTY);
        executionContext.start();

        SegmentDescriptor descriptor = SegmentDescriptor.builder()
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

        SegmentIndex segmentIndex = new SegmentIndex(testFolder.newFile(), (int) descriptor.maxEntries());
        segment = new Segment(streamService, file, segmentIndex, executionContext);
    }

    @After
    public void tearDown() throws Exception {
        executionContext.submit(() -> {
            try {
                segment.close();
            } catch (IOException e) {
                assert false : e;
            }
        }).get();
        executionContext.stop();
        executionContext.close();
    }

    @Test
    public void testAppendEntry() throws Exception {
        executionContext.submit(() -> {
            try {
                for (int i = 0; i < segment.descriptor().maxEntries(); i++) {
                    QueryEntry expected = QueryEntry.builder()
                        .setIndex(i)
                        .setTerm(i)
                        .setTimestamp(random.nextLong())
                        .setVersion(random.nextInt())
                        .setSession(random.nextInt())
                        .setQuery(new PathExists("some path"))
                        .build();

                    segment.appendEntry(expected);
                    QueryEntry actual = segment.getEntry(expected.index());
                    Assert.assertNotNull(actual);

                    Assert.assertEquals(expected.index(), actual.index());
                    Assert.assertEquals(expected.term(), actual.term());
                    Assert.assertEquals(expected.timestamp(), actual.timestamp());
                    Assert.assertEquals(expected.version(), actual.version());
                    Assert.assertEquals(expected.session(), actual.session());

                    Assert.assertEquals(expected.query().consistency(), actual.query().consistency());
                    Assert.assertEquals(((PathExists) expected.query()).path(), ((PathExists) actual.query()).path());
                }

            } catch (IOException e) {
                assert false : e;
            }
        }).get();
    }
}
