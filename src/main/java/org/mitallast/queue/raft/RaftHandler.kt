package org.mitallast.queue.raft

import com.google.inject.Inject
import org.mitallast.queue.raft.protocol.*
import org.mitallast.queue.transport.TransportController

class RaftHandler @Inject constructor(transportController: TransportController, raft: Raft) {
    init {
        transportController.registerMessageHandler(AppendEntries::class.java, raft::apply)
        transportController.registerMessageHandler(AppendRejected::class.java, raft::apply)
        transportController.registerMessageHandler(AppendSuccessful::class.java, raft::apply)

        transportController.registerMessageHandler(AddServer::class.java, raft::apply)
        transportController.registerMessageHandler(AddServerResponse::class.java, raft::apply)
        transportController.registerMessageHandler(RemoveServer::class.java, raft::apply)
        transportController.registerMessageHandler(RemoveServerResponse::class.java, raft::apply)

        transportController.registerMessageHandler(ClientMessage::class.java, raft::apply)

        transportController.registerMessageHandler(InstallSnapshot::class.java, raft::apply)
        transportController.registerMessageHandler(InstallSnapshotRejected::class.java, raft::apply)
        transportController.registerMessageHandler(InstallSnapshotSuccessful::class.java, raft::apply)

        transportController.registerMessageHandler(RequestVote::class.java, raft::apply)
        transportController.registerMessageHandler(VoteCandidate::class.java, raft::apply)
        transportController.registerMessageHandler(DeclineCandidate::class.java, raft::apply)
    }
}