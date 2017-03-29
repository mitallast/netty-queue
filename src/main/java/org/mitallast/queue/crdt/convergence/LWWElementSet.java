package org.mitallast.queue.crdt.convergence;

import gnu.trove.iterator.TObjectLongIterator;
import gnu.trove.map.TObjectLongMap;
import gnu.trove.map.hash.TObjectLongHashMap;

/**
 * An alternative LWW-based approach,6 which we call LWW-element-Assign, attaches a timestamp to each element
 * (rather than to the whole set). Consider add-set A and remove-set R, each containing (element,timestamp) pairs.
 * To add (resp. remove) an element e, add the pair (e,now()), where now was specified earlier, to A (resp. to R).
 * Merging two replicas takes the union of their add-sets and remove-sets.
 * <p>
 * An element e is in the set if it is in A, and it is not in R with a higher
 * timestamp: lookup(e) = ∃t,∀t′ > t : (e,t) ∈ A ∧ (e,t′) ∈/ R).
 * Since it is based on LWW, this data type is convergent.
 */
public class LWWElementSet implements CvRDT<LWWElementSet> {

    public static class Add implements Update<LWWElementSet> {
        private final Object value;
        private final long timestamp;

        public Add(Object value, long timestamp) {
            this.value = value;
            this.timestamp = timestamp;
        }
    }

    public static class Remove implements Update<LWWElementSet> {
        private final Object value;
        private final long timestamp;

        public Remove(Object value, long timestamp) {
            this.value = value;
            this.timestamp = timestamp;
        }
    }

    public static class Lookup implements Query<LWWElementSet> {
        private final Object value;

        public Lookup(Object value) {
            this.value = value;
        }
    }

    public static class LookupResponse implements QueryResponse<LWWElementSet> {
        private final boolean lookup;

        public LookupResponse(boolean lookup) {
            this.lookup = lookup;
        }
    }

    private final TObjectLongMap<Object> A;
    private final TObjectLongMap<Object> R;

    public LWWElementSet() {
        A = new TObjectLongHashMap<>(256, 0.5f, 0);
        R = new TObjectLongHashMap<>(256, 0.5f, 0);
    }

    @Override
    public void update(Update<LWWElementSet> update) {
        if (update instanceof Add) {
            add(((Add) update).value, ((Add) update).timestamp);
        } else if (update instanceof Remove) {
            add(((Remove) update).value, ((Remove) update).timestamp);
        }
    }

    public void add(Object value, long timestamp) {
        A.put(value, timestamp);
    }

    public void remove(Object value, long timestamp) {
        R.put(value, timestamp);
    }

    @Override
    public QueryResponse<LWWElementSet> query(Query<LWWElementSet> query) {
        if (query instanceof Lookup) {
            return new LookupResponse(lookup(((Lookup) query).value));
        }
        return null;
    }

    public boolean lookup(Object value) {
        long added = A.get(value);
        long removed = R.get(value);

        return added > 0 && added > removed;
    }

    @Override
    public boolean compare(LWWElementSet other) {
        TObjectLongIterator<Object> iteratorA = A.iterator();
        for (int i = A.size(); i-- > 0; ) {
            iteratorA.advance();
            if (iteratorA.value() > other.A.get(iteratorA.key())) {
                return true;
            }
        }


        TObjectLongIterator<Object> iteratorR = R.iterator();
        for (int i = R.size(); i-- > 0; ) {
            iteratorR.advance();
            if (iteratorR.value() > other.R.get(iteratorR.key())) {
                return true;
            }
        }

        return false;
    }

    @Override
    public void merge(LWWElementSet other) {
        TObjectLongIterator<Object> iteratorA = other.A.iterator();
        for (int i = other.A.size(); i-- > 0; ) {
            iteratorA.advance();
            if (iteratorA.value() > A.get(iteratorA.key())) {
                A.put(iteratorA.key(), iteratorA.value());
            }
        }

        TObjectLongIterator<Object> iteratorR = other.R.iterator();
        for (int i = other.R.size(); i-- > 0; ) {
            iteratorR.advance();
            if (iteratorR.value() > R.get(iteratorR.key())) {
                R.put(iteratorR.key(), iteratorR.value());
            }
        }
    }
}
