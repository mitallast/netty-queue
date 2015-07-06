package org.mitallast.queue.raft;

public enum ConsistencyLevel {

    SERIALIZABLE {
        @Override
        public boolean isLeaderRequired() {
            return false;
        }
    },

    SEQUENTIAL {
        @Override
        public boolean isLeaderRequired() {
            return false;
        }
    },

    LINEARIZABLE_LEASE {
        @Override
        public boolean isLeaderRequired() {
            return true;
        }
    },

    LINEARIZABLE_STRICT {
        @Override
        public boolean isLeaderRequired() {
            return true;
        }
    };

    public abstract boolean isLeaderRequired();
}
