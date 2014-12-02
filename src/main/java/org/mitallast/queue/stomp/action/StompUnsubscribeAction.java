package org.mitallast.queue.stomp.action;

import com.google.inject.Inject;
import io.netty.handler.codec.stomp.StompCommand;
import io.netty.handler.codec.stomp.StompFrame;
import org.mitallast.queue.client.Client;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.stomp.BaseStompHandler;
import org.mitallast.queue.stomp.StompController;
import org.mitallast.queue.stomp.transport.StompSession;

public class StompUnsubscribeAction extends BaseStompHandler {

    @Inject
    public StompUnsubscribeAction(Settings settings, Client client, StompController controller) {
        super(settings, client);
        controller.registerHandler(StompCommand.UNSUBSCRIBE, this);
    }

    @Override
    public void handleRequest(StompSession session, StompFrame request) {
        session.sendResponse(StompCommand.RECEIPT);
    }
}
