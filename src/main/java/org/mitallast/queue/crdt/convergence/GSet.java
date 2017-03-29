package org.mitallast.queue.crdt.convergence;

import com.google.common.collect.ImmutableSet;
import org.mitallast.queue.common.Immutable;

/**
 * G-Assign.
 * <p>
 * The grow-only set is a CvRDT implementing a set which only allows adds.
 * Since it is impossible for adds and removes to commute (one must take precedence over the other),
 * any CvRDT supporting both add and remove operations must pick and choose its semantics.
 */
public class GSet implements CvRDT<GSet> {

    public static class Add implements Update<GSet> {
        private final Object value;

        public Add(Object value) {
            this.value = value;
        }
    }

    public static class Lookup implements Query<GSet> {
        private final Object value;

        public Lookup(Object value) {
            this.value = value;
        }
    }

    public static class LookupResponse implements QueryResponse<GSet> {
        private final boolean value;

        public LookupResponse(boolean value) {
            this.value = value;
        }
    }

    private ImmutableSet<Object> values = ImmutableSet.of();

    @Override
    public void update(Update<GSet> update) {
        if (update instanceof Add) {
            add(((Add) update).value);
        }
    }

    public void add(Object value) {
        values = Immutable.compose(values, value);
    }

    @Override
    public QueryResponse<GSet> query(Query<GSet> query) {
        if (query instanceof Lookup) {
            return new LookupResponse(lookup(((Lookup) query).value));
        }
        return null;
    }

    public boolean lookup(Object value) {
        return values.contains(value);
    }

    @Override
    public boolean compare(GSet other) {
        return other.values.containsAll(this.values);
    }

    @Override
    public void merge(GSet other) {
        values = Immutable.compose(this.values, other.values);
    }
}
