package org.mitallast.queue.raft.rest;

import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import javaslang.collection.HashMap;
import javaslang.collection.Map;
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

    public Map<String, Object> log() {
        ReplicatedLog log = raft.replicatedLog();

        return HashMap.of(
            "committedIndex", log.committedIndex(),
            "entries", log.entries().map(entry -> HashMap.of(
                "term", entry.getTerm(),
                "index", entry.getIndex(),
                "command", entry.getCommand().getClass().getSimpleName(),
                "client", entry.getClient()
            ))
        );
    }

    public Map<String, Object> state() {
        RaftMetadata meta = raft.currentMeta();
        return HashMap.of(
            "currentTerm", meta.getCurrentTerm(),
            "config", config(meta),
            "votedFor", meta.getVotedFor()
        );
    }

    private Map<String, Object> config(RaftMetadata meta) {
        Map<String, Object> config = HashMap.of(
            "isTransitioning", meta.getConfig().isTransitioning(),
            "members", meta.getConfig().members()
        );
        if (meta.getConfig().isTransitioning()) {
            JointConsensusClusterConfiguration jointConf = (JointConsensusClusterConfiguration) meta.getConfig();
            config = config.put("oldMembers", jointConf.getOldMembers());
            config = config.put("newMembers", jointConf.getNewMembers());
        }
        return config;
    }
}
