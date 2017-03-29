package org.mitallast.queue.raft.rest;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import org.mitallast.queue.common.Immutable;
import org.mitallast.queue.raft.Raft;
import org.mitallast.queue.raft.RaftMetadata;
import org.mitallast.queue.raft.cluster.JointConsensusClusterConfiguration;
import org.mitallast.queue.raft.persistent.ReplicatedLog;
import org.mitallast.queue.rest.RestController;

public class RaftHandler {

    private final Raft raft;

    @Inject
    public RaftHandler(RestController controller, Raft raft) {
        this.raft = raft;
        controller.handler(this::log)
            .response(controller.response().json())
            .handle(HttpMethod.GET, "_raft/log");
        controller.handler(this::state)
            .response(controller.response().json())
            .handle(HttpMethod.GET, "_raft/state");
    }

    public ImmutableMap<String, Object> log() {
        ReplicatedLog log = raft.replicatedLog();

        return ImmutableMap.of(
            "committedIndex", log.committedIndex(),
            "entries", Immutable.map(log.entries(), entry -> ImmutableMap.of(
                "term", entry.getTerm(),
                "index", entry.getIndex(),
                "command", entry.getCommand().getClass().getSimpleName(),
                "client", entry.getClient()
            ))
        );
    }

    public ImmutableMap<String, Object> state() {
        RaftMetadata meta = raft.currentMeta();
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        builder.put("currentTerm", meta.getCurrentTerm());
        builder.put("config", config(meta));
        meta.getVotedFor().ifPresent(votedFor -> builder.put("votedFor", votedFor));
        return builder.build();
    }

    private ImmutableMap<String, Object> config(RaftMetadata meta) {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        builder.put("isTransitioning", meta.getConfig().isTransitioning());
        builder.put("members", meta.getConfig().members());
        if (meta.getConfig().isTransitioning()) {
            JointConsensusClusterConfiguration jointConf = (JointConsensusClusterConfiguration) meta.getConfig();
            builder.put("oldMembers", jointConf.getOldMembers());
            builder.put("newMembers", jointConf.getNewMembers());
        }
        return builder.build();
    }
}
