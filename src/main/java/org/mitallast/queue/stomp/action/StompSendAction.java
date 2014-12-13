package org.mitallast.queue.stomp.action;

import com.google.inject.Inject;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.stomp.StompCommand;
import io.netty.handler.codec.stomp.StompFrame;
import io.netty.handler.codec.stomp.StompHeaders;
import org.mitallast.queue.action.ActionListener;
import org.mitallast.queue.action.queue.enqueue.EnQueueRequest;
import org.mitallast.queue.action.queue.enqueue.EnQueueResponse;
import org.mitallast.queue.client.Client;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.QueueMessage;
import org.mitallast.queue.queue.QueueMessageType;
import org.mitallast.queue.stomp.BaseStompHandler;
import org.mitallast.queue.stomp.StompController;
import org.mitallast.queue.stomp.transport.StompSession;

public class StompSendAction extends BaseStompHandler {

    @Inject
    public StompSendAction(Settings settings, Client client, StompController controller) {
        super(settings, client);
        controller.registerHandler(StompCommand.SEND, this);
    }

    @Override
    public void handleRequest(final StompSession session, final StompFrame request) {
        if (true) {
            session.sendResponse(StompCommand.RECEIPT);
            return;
        }

        String destination = request.headers().get(StompHeaders.DESTINATION);
        String contentType = request.headers().get(StompHeaders.CONTENT_TYPE, "text/plain");
        ByteBuf content = request.content();

        QueueMessage queueMessage = new QueueMessage();
        switch (contentType) {
            case "text":
            case "text/plain":
                queueMessage.setSource(QueueMessageType.STRING, content);
                break;
            case "json":
            case "application/json":
                queueMessage.setSource(QueueMessageType.JSON, content);
                break;
            default:
                session.sendError("Unsupported content type");
                return;
        }

        EnQueueRequest enQueueRequest = new EnQueueRequest();
        enQueueRequest.setQueue(destination);
        enQueueRequest.setMessage(queueMessage);

        client.queue().enqueueRequest(enQueueRequest, new ActionListener<EnQueueResponse>() {
            @Override
            public void onResponse(EnQueueResponse response) {
                session.sendResponse(StompCommand.RECEIPT);
            }

            @Override
            public void onFailure(Throwable e) {
                session.sendError(e);
            }
        });
    }
}
