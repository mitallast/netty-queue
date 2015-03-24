package org.mitallast.queue.stomp.action;

import com.google.inject.Inject;
import io.netty.handler.codec.stomp.StompCommand;
import io.netty.handler.codec.stomp.StompFrame;
import io.netty.handler.codec.stomp.StompHeaders;
import org.mitallast.queue.client.Client;
import org.mitallast.queue.common.Strings;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.queue.Queue;
import org.mitallast.queue.stomp.BaseStompHandler;
import org.mitallast.queue.stomp.StompController;
import org.mitallast.queue.stomp.transport.StompSession;

public class StompSubscribeAction extends BaseStompHandler {

    @Inject
    public StompSubscribeAction(Settings settings, Client client, StompController controller) {
        super(settings, client);
        controller.registerHandler(StompCommand.SUBSCRIBE, this);
    }

    @Override
    public void handleRequest(StompSession session, StompFrame request) {
        String queue = Strings.toString(request.headers().get(StompHeaders.DESTINATION));
        if (Strings.isEmpty(queue)) {
            session.sendError("destination is required");
            return;
        }
        session.subscribe(new Queue(queue));
        session.sendResponse(StompCommand.RECEIPT);
    }
}
