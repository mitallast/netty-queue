package org.mitallast.queue.raft.cluster;

import com.google.common.collect.ImmutableList;
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

}
