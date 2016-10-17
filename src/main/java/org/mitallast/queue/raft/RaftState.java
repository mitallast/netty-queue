package org.mitallast.queue.raft;

public enum RaftState {
    Init,
    Follower,
    Candidate,
    Leader
}
