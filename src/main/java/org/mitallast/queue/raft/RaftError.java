package org.mitallast.queue.raft;

public enum RaftError {

    NO_LEADER_ERROR {
        @Override
        public RaftException createException() {
            return new NoLeaderException("not the leader");
        }
    },

    QUERY_ERROR {
        @Override
        public RaftException createException() {
            return new ReadException("failed to obtain read quorum");
        }
    },

    COMMAND_ERROR {
        @Override
        public RaftException createException() {
            return new WriteException("failed to obtain write quorum");
        }
    },

    APPLICATION_ERROR {
        @Override
        public RaftException createException() {
            return new ApplicationException("an application error occurred");
        }
    },

    ILLEGAL_MEMBER_STATE_ERROR {
        @Override
        public RaftException createException() {
            return new IllegalMemberStateException("illegal member state");
        }
    },

    UNKNOWN_SESSION_ERROR {
        @Override
        public RaftException createException() {
            return new UnknownSessionException("unknown member session");
        }
    },

    INTERNAL_ERROR {
        @Override
        public RaftException createException() {
            return new InternalException("internal Raft error");
        }
    },

    PROTOCOL_ERROR {
        @Override
        public RaftException createException() {
            return new ProtocolException("Raft protocol error");
        }
    };

    abstract public RaftException createException();
}
