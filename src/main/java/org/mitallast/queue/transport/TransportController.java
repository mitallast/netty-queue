package org.mitallast.queue.transport;

import com.google.common.collect.ImmutableMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mitallast.queue.common.Immutable;
import org.mitallast.queue.common.stream.Streamable;

import java.util.function.Consumer;

public class TransportController {
    private final static Logger logger = LogManager.getLogger();

    private volatile ImmutableMap<Class, Consumer> handlerMap = ImmutableMap.of();

    public synchronized <Message extends Streamable> void registerMessageHandler(Class requestClass, Consumer<Message> handler) {
        handlerMap = Immutable.compose(handlerMap, requestClass, handler);
    }

    @SuppressWarnings("unchecked")
    public void dispatch(Streamable message) {
        Consumer handler = handlerMap.get(message.getClass());
        if (handler != null) {
            handler.accept(message);
        } else {
            logger.error("handler not found for {}, close channel", message.getClass());
        }
    }
}
