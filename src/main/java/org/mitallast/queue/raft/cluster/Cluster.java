package org.mitallast.queue.raft.cluster;

public interface Cluster extends Members {

    Member member();

    void addListener(MembershipListener listener);

    void removeListener(MembershipListener listener);
}
