package org.mitallast.queue.common.concurrent.futures;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.mitallast.queue.common.BaseTest;

import java.util.ArrayList;
import java.util.List;

public class FuturesBenchmark extends BaseTest {
    @Rule
    public TestRule benchmarkRule = new BenchmarkRule();

    @Test(timeout = 1000)
    @BenchmarkOptions(callgc = false, benchmarkRounds = 20, warmupRounds = 3)
    public void testSmartFuture() throws Exception {
        final int max = 1000000;
        final List<SmartFuture<String>> futureList = new ArrayList<>(max);
        for (int i = 0; i < max; i++) {
            futureList.add(Futures.future());
        }
        for (SmartFuture<String> future : futureList) {
            future.invoke("foo");
        }
        for (SmartFuture<String> future : futureList) {
            Assert.assertEquals("foo", future.get());
        }
    }
}
