package org.mitallast.queue.transport;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import org.mitallast.queue.common.Immutable;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.netty.codec.MessageTransportFrame;
import org.mitallast.queue.transport.netty.codec.TransportFrame;
import org.mitallast.queue.transport.netty.codec.TransportFrameType;

import java.util.function.Consumer;

public class TransportController extends AbstractComponent {

    private volatile ImmutableMap<Class, Consumer> handlerMap = ImmutableMap.of();

    @Inject
    public TransportController(Config config) {
        super(config.getConfig("transport"), TransportController.class);
    }

    public synchronized <Message extends Streamable> void registerMessageHandler(Class requestClass, Consumer<Message> handler) {
        handlerMap = Immutable.compose(handlerMap, requestClass, handler);
    }

    public void dispatch(TransportFrame messageFrame) {
        if (messageFrame.type() == TransportFrameType.PING) {
            // channel.send(messageFrame);
        } else if (messageFrame.type() == TransportFrameType.MESSAGE) {
            dispatch((MessageTransportFrame) messageFrame);
        }
    }

    @SuppressWarnings("unchecked")
    public void dispatch(MessageTransportFrame messageFrame) {
        Streamable message = messageFrame.message();
        Consumer handler = handlerMap.get(message.getClass());
        if (handler != null) {
            handler.accept(message);
        } else {
            logger.error("handler not found for {}, close channel", message.getClass());
        }
    }
}
