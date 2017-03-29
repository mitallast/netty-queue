package org.mitallast.queue.crdt.convergence;

/**
 * PNCounter.
 * <p>
 * A common strategy in CRDT development is to stick multiple primitive CRDTs together to make a more complex CRDT.
 * In this case, two increment-only counters were combined to create a CvRDT supporting
 * both increment and decrement operations.
 * Note that the CvRDT's internal state must increase monotonically, even though its external state as exposed
 * through query can return to previous values.
 */
public class PNCounter implements CvRDT<PNCounter> {

    public static class Increment implements Update<PNCounter> {
    }

    public static class Decrement implements Update<PNCounter> {
    }

    public static class Value implements Query<PNCounter> {
    }

    public static class ValueResponse implements QueryResponse<PNCounter> {
        private final long value;

        public ValueResponse(long value) {
            this.value = value;
        }
    }

    private final GCounter P;
    private final GCounter N;

    public PNCounter(int n, int i) {
        P = new GCounter(n, i);
        N = new GCounter(n, i);
    }

    @Override
    public void update(Update<PNCounter> update) {
        if (update instanceof Increment) {
            increment();
        } else if (update instanceof Decrement) {
            decrement();
        }
    }

    public void increment() {
        P.increment();
    }

    public void decrement() {
        N.increment();
    }

    @Override
    public QueryResponse<PNCounter> query(Query<PNCounter> query) {
        if (query instanceof Value) {
            return new ValueResponse(value());
        }
        return null;
    }

    public long value() {
        return P.value() - N.value();
    }

    @Override
    public boolean compare(PNCounter other) {
        return P.compare(other.P) && N.compare(other.N);
    }

    @Override
    public void merge(PNCounter other) {
        P.merge(other.P);
        N.merge(other.N);
    }
}