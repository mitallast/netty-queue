package org.mitallast.queue.rest.action.raft;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.common.xstream.XStreamBuilder;
import org.mitallast.queue.raft.Raft;
import org.mitallast.queue.raft.RaftMetadata;
import org.mitallast.queue.raft.cluster.JointConsensusClusterConfiguration;
import org.mitallast.queue.rest.BaseRestHandler;
import org.mitallast.queue.rest.RestController;
import org.mitallast.queue.rest.RestRequest;
import org.mitallast.queue.rest.RestSession;
import org.mitallast.queue.rest.response.ByteBufRestResponse;
import org.mitallast.queue.transport.DiscoveryNode;

import java.io.IOException;
import java.util.Optional;

public class RaftStateAction extends BaseRestHandler {

    private final Raft raft;

    @Inject
    public RaftStateAction(Config config, RestController controller, Raft raft) {
        super(config.getConfig("rest"), RaftStateAction.class);
        this.raft = raft;
        controller.registerHandler(HttpMethod.GET, "/_raft/state", this);
    }

    @Override
    public void handleRequest(RestRequest request, RestSession session) {
        request.content().release();
        RaftMetadata meta = raft.currentMeta();

        ByteBuf buffer = session.alloc().directBuffer();
        try {
            try (XStreamBuilder builder = createBuilder(request, buffer)) {
                builder.writeStartObject();

                builder.writeNumberField("currentTerm", meta.getCurrentTerm().getTerm());
                builder.writeObjectFieldStart("config");

                builder.writeNumberField("sequenceNumber", meta.getConfig().sequenceNumber());
                builder.writeNumberField("quorum", meta.getConfig().quorum());
                builder.writeBooleanField("isTransitioning", meta.getConfig().isTransitioning());

                builder.writeArrayFieldStart("members");
                for (DiscoveryNode discoveryNode : meta.getConfig().members()) {
                    builder.writeStartObject();
                    builder.writeStringField("host", discoveryNode.host());
                    builder.writeNumberField("port", discoveryNode.port());
                    builder.writeEndObject();
                }
                builder.writeEndArray();

                if (meta.getConfig().isTransitioning()) {
                    JointConsensusClusterConfiguration jointConf = (JointConsensusClusterConfiguration) meta.getConfig();

                    builder.writeArrayFieldStart("oldMembers");
                    for (DiscoveryNode discoveryNode : jointConf.getOldMembers()) {
                        builder.writeStartObject();
                        builder.writeStringField("host", discoveryNode.host());
                        builder.writeNumberField("port", discoveryNode.port());
                        builder.writeEndObject();
                    }
                    builder.writeEndArray();

                    builder.writeArrayFieldStart("newMembers");
                    for (DiscoveryNode discoveryNode : jointConf.getNewMembers()) {
                        builder.writeStartObject();
                        builder.writeStringField("host", discoveryNode.host());
                        builder.writeNumberField("port", discoveryNode.port());
                        builder.writeEndObject();
                    }
                    builder.writeEndArray();
                }

                builder.writeEndObject(); // end config

                Optional<DiscoveryNode> votedFor = meta.getVotedFor();
                if (votedFor.isPresent()) {
                    builder.writeObjectFieldStart("votedFor");
                    builder.writeStringField("host", votedFor.get().host());
                    builder.writeNumberField("port", votedFor.get().port());
                    builder.writeEndObject();
                }

                builder.writeNumberField("votesReceived", meta.getVotesReceived());

                builder.writeEndObject();
            }
            session.sendResponse(new ByteBufRestResponse(HttpResponseStatus.OK, buffer));
        } catch (IOException e) {
            session.sendResponse(e);
        }
    }
}
