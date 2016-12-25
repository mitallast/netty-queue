package org.mitallast.queue.rest.action;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.common.xstream.XStreamBuilder;
import org.mitallast.queue.common.xstream.XStreamString;
import org.mitallast.queue.rest.BaseRestHandler;
import org.mitallast.queue.rest.RestController;
import org.mitallast.queue.rest.RestRequest;
import org.mitallast.queue.rest.RestSession;
import org.mitallast.queue.rest.response.ByteBufRestResponse;

import java.io.IOException;

public class SettingsAction extends BaseRestHandler {

    private final static XStreamString RAFT = new XStreamString("raft");
    private final static XStreamString BLOB = new XStreamString("blob");
    private final static XStreamString BENCHMARK = new XStreamString("benchmark");
    private final static XStreamString ENABLED = new XStreamString("enabled");

    @Inject
    public SettingsAction(Config config, RestController controller) {
        super(config, SettingsAction.class);
        controller.registerHandler(HttpMethod.GET, "/_settings", this);
    }

    @Override
    public void handleRequest(RestRequest request, RestSession session) {
        request.content().release();
        ByteBuf buffer = session.alloc().directBuffer();
        try {
            try (XStreamBuilder builder = createBuilder(request, buffer)) {
                builder.writeStartObject();

                builder.writeObjectFieldStart(RAFT);
                builder.writeBooleanField(ENABLED, config.getBoolean("raft.enabled"));
                builder.writeEndObject();

                builder.writeObjectFieldStart(BLOB);
                builder.writeBooleanField(ENABLED, config.getBoolean("blob.enabled"));
                builder.writeEndObject();

                builder.writeObjectFieldStart(BENCHMARK);
                builder.writeBooleanField(ENABLED, config.getBoolean("benchmark.enabled"));
                builder.writeEndObject();

                builder.writeEndObject();
            }
            session.sendResponse(new ByteBufRestResponse(HttpResponseStatus.OK, buffer));
        } catch (IOException e) {
            session.sendResponse(e);
        }
    }
}
