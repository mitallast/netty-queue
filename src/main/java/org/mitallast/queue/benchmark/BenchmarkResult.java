package org.mitallast.queue.benchmark;

public class BenchmarkResult {
    private final int requests;
    private final int dataSize;

    private final long start;
    private final long end;

    public BenchmarkResult(int requests, int dataSize, long start, long end) {
        this.requests = requests;
        this.dataSize = dataSize;
        this.start = start;
        this.end = end;
    }

    public int getRequests() {
        return requests;
    }

    public int getDataSize() {
        return dataSize;
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }

    @Override
    public String toString() {
        return "BenchmarkResult{" +
            "requests=" + requests +
            ", dataSize=" + dataSize +
            ", start=" + start +
            ", end=" + end +
            '}';
    }
}
