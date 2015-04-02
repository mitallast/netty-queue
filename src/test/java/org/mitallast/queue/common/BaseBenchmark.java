package org.mitallast.queue.common;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkOptionsSystemProperties;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;
import org.junit.Rule;
import org.junit.rules.TestRule;

@AxisRange(min = 0, max = 1)
@BenchmarkOptions(callgc = true, warmupRounds = 2, benchmarkRounds = 6)
@BenchmarkMethodChart(filePrefix = "benchmark-lists")
@BenchmarkHistoryChart(labelWith = LabelType.CUSTOM_KEY, maxRuns = 20)
public class BaseBenchmark extends BaseTest {
    static {
        System.setProperty(BenchmarkOptionsSystemProperties.DB_FILE_PROPERTY, "target/benchmarks/benchmarks");
        System.setProperty(BenchmarkOptionsSystemProperties.CHARTS_DIR_PROPERTY, "target/benchmarks");
        System.setProperty(BenchmarkOptionsSystemProperties.CONSUMERS_PROPERTY, "CONSOLE,H2");
        System.setProperty(BenchmarkOptionsSystemProperties.WARMUP_ROUNDS_PROPERTY, "3");
        System.setProperty(BenchmarkOptionsSystemProperties.BENCHMARK_ROUNDS_PROPERTY, "10");
    }

    @Rule
    public TestRule benchmarkRule = new BenchmarkRule();
}
