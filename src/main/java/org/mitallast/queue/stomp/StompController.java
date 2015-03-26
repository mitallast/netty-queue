package org.mitallast.queue.stomp;

import com.google.inject.Inject;
import io.netty.handler.codec.stomp.StompCommand;
import io.netty.handler.codec.stomp.StompFrame;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.stomp.transport.StompSession;

/**
 * @link http://stomp.github.io/stomp-specification-1.2.html#Protocol_Overview
 */
public class StompController extends AbstractComponent {

    private StompHandler[] handlers = new StompHandler[StompCommand.values().length];

    @Inject
    public StompController(Settings settings) {
        super(settings);
    }

    public synchronized void registerHandler(StompCommand command, StompHandler handler) {
        handlers[command.ordinal()] = handler;
    }

    public void dispatchRequest(StompSession session, StompFrame request) {
        StompHandler handler = handlers[request.command().ordinal()];
        handler.handleRequest(session, request);
    }
}
