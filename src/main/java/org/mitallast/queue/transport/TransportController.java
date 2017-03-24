package org.mitallast.queue.transport;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.protobuf.Message;
import com.typesafe.config.Config;
import org.mitallast.queue.common.Immutable;
import org.mitallast.queue.common.component.AbstractComponent;

import java.util.function.Consumer;

public class TransportController extends AbstractComponent {

    private volatile ImmutableMap<Class, Consumer> handlerMap = ImmutableMap.of();

    @Inject
    public TransportController(Config config) {
        super(config.getConfig("transport"), TransportController.class);
    }

    public synchronized <M extends com.google.protobuf.Message> void registerMessageHandler(Class<M> requestClass, Consumer<M> handler) {
        handlerMap = Immutable.compose(handlerMap, requestClass, handler);
    }

    @SuppressWarnings("unchecked")
    public void dispatch(Message message) {
        Consumer handler = handlerMap.get(message.getClass());
        if (handler != null) {
            handler.accept(message);
        } else {
            logger.error("handler not found for {}, close channel", message.getClass());
        }
    }
}
