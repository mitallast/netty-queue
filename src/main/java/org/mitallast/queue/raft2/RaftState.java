package org.mitallast.queue.raft2;

public enum RaftState {
    Init,
    Follower,
    Candidate,
    Leader
}
