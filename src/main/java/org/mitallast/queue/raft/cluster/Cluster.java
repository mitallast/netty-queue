package org.mitallast.queue.raft.cluster;

import org.mitallast.queue.action.ActionRequest;

public interface Cluster extends Members {

    Member member();

    <T extends ActionRequest> void broadcast(T message);

    <T> void broadcast(Class<? super T> type, T message);

    <T> void broadcast(String topic, T message);

    void addListener(MembershipListener listener);

    void removeListener(MembershipListener listener);
}
