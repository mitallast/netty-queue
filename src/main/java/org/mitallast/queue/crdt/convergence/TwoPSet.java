package org.mitallast.queue.crdt.convergence;

/**
 * 2P-Assign.
 * <p>
 * Two grow-only set CvRDTs are combined to create the 2P-set CvRDT.
 * With the addition of a "tombstone" set, elements can be added and also removed.
 * Once removed, an element cannot be re-added; that is, once an element e is in the tombstone set,
 * query will never again return True for that element. The 2P-set uses "remove-wins" semantics,
 * so remove(e) takes precedence over add(e).
 */
public class TwoPSet implements CvRDT<TwoPSet> {

    public static class Add implements Update<TwoPSet> {
        private final Object value;

        public Add(Object value) {
            this.value = value;
        }
    }

    public static class Remove implements Update<TwoPSet> {
        private final Object value;

        public Remove(Object value) {
            this.value = value;
        }
    }

    public static class Lookup implements Query<TwoPSet> {
        private final Object value;

        public Lookup(Object value) {
            this.value = value;
        }
    }

    public static class LookupResponse implements QueryResponse<TwoPSet> {
        private final boolean value;

        public LookupResponse(boolean value) {
            this.value = value;
        }
    }

    private GSet A;
    private GSet R;

    public TwoPSet() {
        A = new GSet();
        R = new GSet();
    }

    @Override
    public void update(Update<TwoPSet> update) {
        if (update instanceof Add) {
            add(((Add) update).value);
        } else if (update instanceof Remove) {
            remove(((Remove) update).value);
        }
    }

    public void add(Object value) {
        A.add(value);
    }

    public void remove(Object value) {
        R.add(value);
    }

    @Override
    public QueryResponse<TwoPSet> query(Query<TwoPSet> query) {
        if (query instanceof Lookup) {
            return new LookupResponse(lookup(((Lookup) query).value));
        }
        return null;
    }

    public boolean lookup(Object value) {
        return A.lookup(value) && !R.lookup(value);
    }

    @Override
    public boolean compare(TwoPSet other) {
        return A.compare(other.A) && R.compare(other.R);
    }

    @Override
    public void merge(TwoPSet other) {
        A.merge(other.A);
        R.merge(other.R);
    }
}
