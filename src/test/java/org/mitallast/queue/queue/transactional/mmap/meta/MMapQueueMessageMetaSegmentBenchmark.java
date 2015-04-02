package org.mitallast.queue.queue.transactional.mmap.meta;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mitallast.queue.common.BaseBenchmark;
import org.mitallast.queue.common.mmap.MemoryMappedFile;
import org.mitallast.queue.queue.QueueMessageStatus;
import org.mitallast.queue.queue.QueueMessageType;

import java.util.ArrayList;
import java.util.List;

@AxisRange(min = 0, max = 1)
@BenchmarkOptions(callgc = true, warmupRounds = 2, benchmarkRounds = 6)
@BenchmarkMethodChart(filePrefix = "benchmark-lists")
@BenchmarkHistoryChart(labelWith = LabelType.CUSTOM_KEY, maxRuns = 20)
public class MMapQueueMessageMetaSegmentBenchmark extends BaseBenchmark {

    private MemoryMappedFile mmapFile;
    private List<QueueMessageMeta> metaList;
    private MMapQueueMessageMetaSegment messageMetaSegment;

    @Before
    public void setUp() throws Exception {
        mmapFile = new MemoryMappedFile(testFolder.newFile());
        messageMetaSegment = new MMapQueueMessageMetaSegment(mmapFile, total(), 0.7f);
        metaList = new ArrayList<>(total());
        for (int i = 0; i < total(); i++) {
            metaList.add(meta());
        }
    }

    @After
    public void tearDown() throws Exception {
        mmapFile.close();
    }

    @Test
    public void testWrite() throws Exception {
        for (QueueMessageMeta meta : metaList) {
            assert messageMetaSegment.insert(meta.getUuid());
            assert messageMetaSegment.writeLock(meta.getUuid());
            assert messageMetaSegment.writeMeta(meta);
        }
    }

    private QueueMessageMeta meta() {
        return new QueueMessageMeta(
            randomUUID(),
            QueueMessageStatus.QUEUED,
            random.nextInt(),
            random.nextInt(),
            QueueMessageType.STRING
        );
    }
}
