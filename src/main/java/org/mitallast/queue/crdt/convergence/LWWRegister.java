package org.mitallast.queue.crdt.convergence;

public class LWWRegister implements CvRDT<LWWRegister> {
    public static class Assign implements Update<LWWRegister> {
        private final Object value;
        private final long timestamp;

        public Assign(Object value, long timestamp) {
            this.value = value;
            this.timestamp = timestamp;
        }
    }

    public static class Value implements Query<LWWRegister> {
    }

    public static class ValueResponse implements QueryResponse<LWWRegister> {
        private final Object value;

        public ValueResponse(Object value) {
            this.value = value;
        }
    }

    private Object value = null;
    private long timestamp = 0;

    @Override
    public void update(Update<LWWRegister> update) {
        if (update instanceof Assign) {
            assign(((Assign) update).value, ((Assign) update).timestamp);
        }
    }

    public void assign(Object value, long timestamp) {
        if (timestamp > this.timestamp) {
            this.value = value;
            this.timestamp = timestamp;
        }
    }

    @Override
    public QueryResponse<LWWRegister> query(Query<LWWRegister> query) {
        if (query instanceof Value) {
            return new ValueResponse(value());
        }
        return null;
    }

    public Object value() {
        return value;
    }

    @Override
    public boolean compare(LWWRegister other) {
        return this.timestamp <= other.timestamp;
    }

    @Override
    public void merge(LWWRegister other) {
        if (this.timestamp <= other.timestamp) {
            this.value = other.value;
            this.timestamp = other.timestamp;
        }
    }
}
