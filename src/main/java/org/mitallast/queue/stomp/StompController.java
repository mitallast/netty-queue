package org.mitallast.queue.stomp;

import com.google.inject.Inject;
import io.netty.handler.codec.stomp.StompCommand;
import io.netty.handler.codec.stomp.StompFrame;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.stomp.transport.StompSession;

import java.util.HashMap;
import java.util.Map;

/**
 * @link http://stomp.github.io/stomp-specification-1.2.html#Protocol_Overview
 */
public class StompController extends AbstractComponent {

    private final Map<StompCommand, StompHandler> handlers = new HashMap<>();

    @Inject
    public StompController(Settings settings) {
        super(settings);
    }

    public synchronized void registerHandler(StompCommand command, StompHandler handler) {
        handlers.put(command, handler);
    }

    public void dispatchRequest(StompSession session, StompFrame request) {
        logger.info("frame received: {}", request);
        StompHandler handler = handlers.get(request.command());
        handler.handleRequest(session, request);
    }
}
