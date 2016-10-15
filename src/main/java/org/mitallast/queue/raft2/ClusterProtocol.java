package org.mitallast.queue.raft2;

import org.mitallast.queue.transport.DiscoveryNode;

public interface ClusterProtocol {
    class RaftMembersDiscoveryTimeout{}
    class RaftMembersDiscoveryRequest{}
    class RaftMembersDiscoveryResponse{}
    class RaftMemberAdded{
        private final DiscoveryNode member;
        private final int keepInitUntil;

        public RaftMemberAdded(DiscoveryNode member, int keepInitUntil) {
            this.member = member;
            this.keepInitUntil = keepInitUntil;
        }

        public DiscoveryNode getMember() {
            return member;
        }

        public int getKeepInitUntil() {
            return keepInitUntil;
        }
    }
    class RaftMemberRemoved{
        private final DiscoveryNode member;
        private final int keepInitUntil;

        public RaftMemberRemoved(DiscoveryNode member, int keepInitUntil) {
            this.member = member;
            this.keepInitUntil = keepInitUntil;
        }

        public DiscoveryNode getMember() {
            return member;
        }

        public int getKeepInitUntil() {
            return keepInitUntil;
        }
    }
}
