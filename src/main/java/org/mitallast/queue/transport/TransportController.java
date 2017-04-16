package org.mitallast.queue.transport;

import javaslang.collection.HashMap;
import javaslang.collection.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mitallast.queue.common.stream.Streamable;

public class TransportController {
    private final static Logger logger = LogManager.getLogger();

    private volatile Map<Class, TransportHandler> handlerMap = HashMap.empty();

    public synchronized <Message extends Streamable> void registerMessageHandler(Class requestClass, TransportHandler<Message> handler) {
        handlerMap = handlerMap.put(requestClass, handler);
    }

    @SuppressWarnings("unchecked")
    public void dispatch(Streamable message) {
        TransportHandler handler = handlerMap.getOrElse(message.getClass(), null);
        if (handler != null) {
            handler.handle(message);
        } else {
            logger.error("handler not found for {}, close channel", message.getClass());
        }
    }
}
