package org.mitallast.queue.raft.rest

import com.google.inject.Inject
import io.netty.handler.codec.http.HttpMethod
import io.vavr.collection.HashMap
import io.vavr.collection.Map
import org.mitallast.queue.raft.Raft
import org.mitallast.queue.raft.RaftMetadata
import org.mitallast.queue.raft.cluster.JointConsensusClusterConfiguration
import org.mitallast.queue.rest.RestController

class RaftHandler @Inject constructor(controller: RestController, private val raft: Raft) {

    init {
        controller
            .handle(this::log, controller.response().json())
            .handle(HttpMethod.GET, "_raft/log")
        controller
            .handle(this::state, controller.response().json())
            .handle(HttpMethod.GET, "_raft/state")
    }

    fun log(): Map<String, Any> {
        val log = raft.replicatedLog()
        val entries = log.entries().map { (term, index, session, command) ->
            HashMap.of(
                "term", term,
                "index", index,
                "command", command.javaClass.simpleName,
                "session", session
            )
        }
        return HashMap.of(
            "committedIndex", log.committedIndex(),
            "entries", entries
        )
    }

    fun state(): Map<String, Any> {
        val meta = raft.currentMeta()
        return HashMap.of(
            "currentTerm", meta.currentTerm,
            "config", config(meta),
            "votedFor", meta.votedFor
        )
    }

    private fun config(meta: RaftMetadata): Map<String, Any> {
        var config: Map<String, Any> = HashMap.of(
            "isTransitioning", meta.config.isTransitioning,
            "members", meta.config.members
        )
        if (meta.config.isTransitioning) {
            val (oldMembers, newMembers) = meta.config as JointConsensusClusterConfiguration
            config = config.put("oldMembers", oldMembers)
            config = config.put("newMembers", newMembers)
        }
        return config
    }
}
