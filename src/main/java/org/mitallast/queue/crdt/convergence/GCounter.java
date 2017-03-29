package org.mitallast.queue.crdt.convergence;

/**
 * G-Counter
 * <p>
 * This CvRDT implements a counter for a cluster of n nodes.
 * Each node in the cluster is assigned an ID from 0 to n - 1,
 * which is retrieved with a call to myId().
 * Thus each node is assigned its own slot in the array P, which it increments locally.
 * Updates are propagated in the background, and merged by taking the max() of every element in P.
 * The compare function is included to illustrate a partial order on the states.
 * The merge function is commutative, associative, and idempotent.
 * The update function monotonically increases the internal state according to the compare function.
 * This is thus a correctly-defined CvRDT and will provide strong eventual consistency.
 * The CmRDT equivalent broadcasts increment operations as they are received.
 */
public class GCounter implements CvRDT<GCounter> {

    public static class Increment implements Update<GCounter> {
    }

    public static class Value implements Query<GCounter> {
    }

    public static class ValueResponse implements QueryResponse<GCounter> {
        private final long value;

        public ValueResponse(long value) {
            this.value = value;
        }
    }

    private final int N;
    private final int index;
    private final long[] values;

    public GCounter(int n, int i) {
        N = n;
        index = i;
        values = new long[N];
    }

    @Override
    public void update(Update<GCounter> update) {
        if (update instanceof Increment) {
            increment();
        }
    }

    public void increment() {
        values[index] = values[index] + 1;
    }

    @Override
    public QueryResponse<GCounter> query(Query<GCounter> query) {
        if (query instanceof Value) {
            return new ValueResponse(value());
        }
        return null;
    }

    public long value() {
        long sum = 0;
        for (long value : values) {
            sum += value;
        }
        return sum;
    }

    @Override
    public boolean compare(GCounter other) {
        for (int i = 0; i < N; i++) {
            if (this.values[i] > other.values[i]) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void merge(GCounter other) {
        for (int i = 0; i < N; i++) {
            this.values[i] = Math.max(this.values[i], other.values[i]);
        }
    }
}
