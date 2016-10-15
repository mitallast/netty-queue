package org.mitallast.queue.raft2;

public enum State {
    Init,
    Follower,
    Candidate,
    Leader
}
