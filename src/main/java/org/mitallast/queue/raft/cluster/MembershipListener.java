package org.mitallast.queue.raft.cluster;

public interface MembershipListener {

    void memberJoined(Member member);

    void memberLeft(Member member);

}
