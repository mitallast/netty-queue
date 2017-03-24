package org.mitallast.queue.raft.rest;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.common.xstream.XStreamBuilder;
import org.mitallast.queue.proto.raft.JointConsensusClusterConfiguration;
import org.mitallast.queue.raft.Raft;
import org.mitallast.queue.raft.RaftMetadata;
import org.mitallast.queue.rest.BaseRestHandler;
import org.mitallast.queue.rest.RestController;
import org.mitallast.queue.rest.RestRequest;
import org.mitallast.queue.rest.RestSession;
import org.mitallast.queue.rest.response.ByteBufRestResponse;
import org.mitallast.queue.proto.raft.DiscoveryNode;

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

                builder.writeNumberField("currentTerm", meta.getCurrentTerm());
                builder.writeObjectFieldStart("config");

                builder.writeBooleanField("isTransitioning", meta.isTransitioning());

                builder.writeArrayFieldStart("members");
                for (DiscoveryNode discoveryNode : meta.members()) {
                    builder.writeStartObject();
                    builder.writeStringField("host", discoveryNode.getHost());
                    builder.writeNumberField("port", discoveryNode.getPort());
                    builder.writeEndObject();
                }
                builder.writeEndArray();

                if (meta.isTransitioning()) {
                    JointConsensusClusterConfiguration jointConf = meta.getConfig().getJoint();

                    builder.writeArrayFieldStart("oldMembers");
                    for (DiscoveryNode discoveryNode : jointConf.getOldMembersList()) {
                        builder.writeStartObject();
                        builder.writeStringField("host", discoveryNode.getHost());
                        builder.writeNumberField("port", discoveryNode.getPort());
                        builder.writeEndObject();
                    }
                    builder.writeEndArray();

                    builder.writeArrayFieldStart("newMembers");
                    for (DiscoveryNode discoveryNode : jointConf.getNewMembersList()) {
                        builder.writeStartObject();
                        builder.writeStringField("host", discoveryNode.getHost());
                        builder.writeNumberField("port", discoveryNode.getPort());
                        builder.writeEndObject();
                    }
                    builder.writeEndArray();
                }

                builder.writeEndObject(); // end config

                Optional<DiscoveryNode> votedFor = meta.getVotedFor();
                if (votedFor.isPresent()) {
                    builder.writeObjectFieldStart("votedFor");
                    builder.writeStringField("host", votedFor.get().getHost());
                    builder.writeNumberField("port", votedFor.get().getPort());
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
