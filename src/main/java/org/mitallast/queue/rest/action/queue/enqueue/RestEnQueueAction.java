package org.mitallast.queue.rest.action.queue.enqueue;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.mitallast.queue.QueueParseException;
import org.mitallast.queue.action.ActionListener;
import org.mitallast.queue.action.queue.enqueue.EnQueueRequest;
import org.mitallast.queue.action.queue.enqueue.EnQueueResponse;
import org.mitallast.queue.client.Client;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queue.QueueMessageUuidDuplicateException;
import org.mitallast.queue.rest.BaseRestHandler;
import org.mitallast.queue.rest.RestController;
import org.mitallast.queue.rest.RestRequest;
import org.mitallast.queue.rest.RestSession;
import org.mitallast.queue.rest.response.StringRestResponse;

import java.io.IOException;
import java.io.InputStream;

public class RestEnQueueAction extends BaseRestHandler {

    @Inject
    public RestEnQueueAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(HttpMethod.POST, "/{queue}/message", this);
        controller.registerHandler(HttpMethod.PUT, "/{queue}/message", this);
    }

    private void parse(EnQueueRequest request, InputStream inputStream) throws IOException {
        try (JsonParser parser = createParser(inputStream)) {
            String currentFieldName;
            JsonToken token;

            token = parser.nextToken();
            if (token == null) {
                throw new QueueParseException("malformed, expected settings to start with 'object', actual [null]");
            }
            if (token != JsonToken.START_OBJECT) {
                throw new QueueParseException("malformed, expected settings to start with 'object', actual [" + token + "]");
            }

            while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {
                if (token == JsonToken.FIELD_NAME) {
                    currentFieldName = parser.getCurrentName();
                    switch (currentFieldName) {
                        case "message":
                            token = parser.nextToken();
                            if (token == JsonToken.VALUE_STRING) {
                                request.getMessage().setSource(parser.getText());
                            } else if (token == JsonToken.START_OBJECT || token == JsonToken.START_ARRAY) {
                                request.getMessage().setSource(parser.readValueAsTree());
                            } else {
                                throw new QueueParseException("malformed, expected string, object or array value at field [" + currentFieldName + "]");
                            }
                            break;
                        case "uuid":
                            token = parser.nextToken();
                            if (token == JsonToken.VALUE_STRING) {
                                request.getMessage().setUuid(parser.getText());
                            } else {
                                throw new QueueParseException("malformed, expected string value at field [" + currentFieldName + "]");
                            }
                            break;
                        default:
                            throw new QueueParseException("malformed, unexpected field [" + currentFieldName + "]");
                    }
                }
            }
        }
    }

    @Override
    public void handleRequest(RestRequest request, final RestSession session) {
        QueueMessage queueMessage = new QueueMessage();

        final EnQueueRequest enQueueRequest = new EnQueueRequest();
        enQueueRequest.setQueue(request.param("queue"));
        enQueueRequest.setMessage(queueMessage);

        try (InputStream stream = request.getInputStream()) {
            parse(enQueueRequest, stream);
        } catch (IOException e) {
            session.sendResponse(e);
            return;
        }

        client.queue().enqueueRequest(enQueueRequest, new ActionListener<EnQueueResponse>() {

            @Override
            public void onResponse(EnQueueResponse response) {
                session.sendResponse(new StringRestResponse(HttpResponseStatus.CREATED, response.getUUID().toString()));
            }

            @Override
            public void onFailure(Throwable e) {
                if (e instanceof QueueMessageUuidDuplicateException) {
                    session.sendResponse(new StringRestResponse(HttpResponseStatus.CONFLICT, "Message already exists"));
                } else {
                    session.sendResponse(e);
                }
            }
        });
    }
}
