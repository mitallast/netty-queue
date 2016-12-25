package org.mitallast.queue.raft.rest;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.common.xstream.XStreamBuilder;
import org.mitallast.queue.raft.Raft;
import org.mitallast.queue.raft.log.ReplicatedLog;
import org.mitallast.queue.raft.protocol.LogEntry;
import org.mitallast.queue.rest.BaseRestHandler;
import org.mitallast.queue.rest.RestController;
import org.mitallast.queue.rest.RestRequest;
import org.mitallast.queue.rest.RestSession;
import org.mitallast.queue.rest.response.ByteBufRestResponse;

import java.io.IOException;

public class RaftLogAction extends BaseRestHandler {

    private final Raft raft;

    @Inject
    public RaftLogAction(Config config, RestController controller, Raft raft) {
        super(config.getConfig("rest"), RaftStateAction.class);
        this.raft = raft;
        controller.registerHandler(HttpMethod.GET, "/_raft/log", this);
    }

    @Override
    public void handleRequest(RestRequest request, RestSession session) {
        request.content().release();
        ReplicatedLog log = raft.replicatedLog();

        ByteBuf buffer = session.alloc().directBuffer();
        try {
            try (XStreamBuilder builder = createBuilder(request, buffer)) {
                builder.writeStartObject();

                builder.writeNumberField("committedIndex", log.committedIndex());

                builder.writeArrayFieldStart("entries");
                for (LogEntry logEntry : log.entries()) {
                    builder.writeStartObject();
                    builder.writeNumberField("term", logEntry.getTerm().getTerm());
                    builder.writeNumberField("index", logEntry.getIndex());
                    builder.writeStringField("command", logEntry.getCommand().getClass().getSimpleName());
                    builder.writeObjectFieldStart("client");
                    builder.writeStringField("host", logEntry.getClient().host());
                    builder.writeNumberField("port", logEntry.getClient().port());
                    builder.writeEndObject();
                    builder.writeEndObject();
                }
                builder.writeEndArray();
                builder.writeEndObject();
            }
            session.sendResponse(new ByteBufRestResponse(HttpResponseStatus.OK, buffer));
        } catch (IOException e) {
            session.sendResponse(e);
        }
    }
}
