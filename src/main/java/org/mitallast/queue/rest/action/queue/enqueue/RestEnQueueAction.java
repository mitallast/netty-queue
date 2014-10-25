package org.mitallast.queue.rest.action.queue.enqueue;

import com.fasterxml.jackson.core.JsonFactory;
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
import org.mitallast.queue.rest.response.HeaderRestResponse;
import org.mitallast.queue.rest.response.StringRestResponse;
import org.mitallast.queue.rest.support.Headers;

import java.io.IOException;
import java.io.InputStream;

public class RestEnQueueAction extends BaseRestHandler {

    @Inject
    public RestEnQueueAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(HttpMethod.POST, "/{queue}/message", this);
    }

    private static void parse(EnQueueRequest<String> request, InputStream inputStream) throws IOException {
        JsonFactory jsonFactory = new JsonFactory();
        JsonParser parser = jsonFactory.createParser(inputStream);

        String currentFieldName;
        JsonToken token;

        token = parser.nextToken();
        if (token == null) {
            throw new QueueParseException("malformed, expected settings to start with 'object', instead was [null]");
        }
        if (token != JsonToken.START_OBJECT) {
            throw new QueueParseException("malformed, expected settings to start with 'object', instead was [" + token + "]");
        }

        while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {
            if (token == JsonToken.FIELD_NAME) {
                currentFieldName = parser.getCurrentName();
                switch (currentFieldName) {
                    case "message":
                        token = parser.nextToken();
                        if (token == JsonToken.VALUE_STRING) {
                            request.getMessage().setMessage(parser.getText());
                        } else {
                            throw new QueueParseException("malformed, expected string value at field [" + currentFieldName + "]");
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

    @Override
    public void handleRequest(RestRequest request, final RestSession session) {
        QueueMessage<String> queueMessage = new QueueMessage<>();

        final EnQueueRequest<String> enQueueRequest = new EnQueueRequest<>();
        enQueueRequest.setQueue(request.param("queue"));
        enQueueRequest.setMessage(queueMessage);

        try (InputStream stream = request.getInputStream()) {
            parse(enQueueRequest, stream);
        } catch (IOException | QueueMessageUuidDuplicateException e) {
            session.sendResponse(e);
            return;
        }

        client.queue().enQueueRequest(enQueueRequest, new ActionListener<EnQueueResponse>() {

            @Override
            public void onResponse(EnQueueResponse response) {
                session.sendResponse(new HeaderRestResponse(HttpResponseStatus.CREATED, Headers.MESSAGE_INDEX, response.getUUID()));
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
