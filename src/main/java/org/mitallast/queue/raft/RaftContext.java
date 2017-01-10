package org.mitallast.queue.raft;

public interface RaftContext {

    String ELECTION_TIMEOUT = "election-timeout";
    String SEND_HEARTBEAT = "send-heartbeat";

    void setTimer(String name, long delayMs, Runnable task);

    void startTimer(String name, long delayMs, long periodMs, Runnable task);

    void cancelTimer(String name);
}
