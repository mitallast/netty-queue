package org.mitallast.queue.raft.cluster;

import com.google.common.collect.ImmutableList;
import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.common.settings.Settings;

import java.util.Collection;

public abstract class AbstractCluster extends AbstractMembers implements Cluster {
    protected final Member localMember;

    protected AbstractCluster(Settings settings, Member localMember, Collection<Member> members) {
        super(settings, ImmutableList.<Member>builder().add(localMember).addAll(members).build());
        this.localMember = localMember;
    }

    @Override
    public Member member() {
        return localMember;
    }

    @Override
    public <T extends ActionRequest> void broadcast(T message) {
        members.values().forEach(m -> {
            m.send(message);
        });
    }

    @Override
    public <T> void broadcast(Class<? super T> type, T message) {
        members.values().forEach(m -> {
            m.send(type, message);
        });
    }

    @Override
    public <T> void broadcast(String topic, T message) {
        members.values().forEach(m -> {
            m.send(topic, message);
        });
    }
}
