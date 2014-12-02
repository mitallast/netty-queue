package org.mitallast.queue.stomp.action;

import com.google.inject.Inject;
import io.netty.handler.codec.stomp.DefaultStompFrame;
import io.netty.handler.codec.stomp.StompCommand;
import io.netty.handler.codec.stomp.StompFrame;
import io.netty.handler.codec.stomp.StompHeaders;
import org.mitallast.queue.client.Client;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.stomp.BaseStompHandler;
import org.mitallast.queue.stomp.StompController;
import org.mitallast.queue.stomp.transport.StompSession;

public class StompConnectAction extends BaseStompHandler {

    @Inject
    public StompConnectAction(Settings settings, Client client, StompController controller) {
        super(settings, client);
        controller.registerHandler(StompCommand.CONNECT, this);
        controller.registerHandler(StompCommand.STOMP, this);
    }

    @Override
    public void handleRequest(StompSession session, StompFrame request) {
        StompFrame response = new DefaultStompFrame(StompCommand.CONNECTED);
        response.headers().set(StompHeaders.VERSION, "1.2");
        session.sendResponse(response);
    }
}
