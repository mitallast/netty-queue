package org.mitallast.queue.transport;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.mitallast.queue.common.Immutable;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.common.stream.Streamable;
import org.mitallast.queue.transport.netty.codec.MessageTransportFrame;

import java.util.function.BiConsumer;

public class TransportController extends AbstractComponent {

    private volatile ImmutableMap<Class, BiConsumer> handlerMap = ImmutableMap.of();

    @Inject
    public TransportController(Settings settings) {
        super(settings);
    }

    public synchronized <Message extends Streamable> void registerMessageHandler(Class requestClass, BiConsumer<TransportChannel, Message> handler) {
        handlerMap = Immutable.compose(handlerMap, requestClass, handler);
    }

    @SuppressWarnings("unchecked")
    public void dispatchMessage(TransportChannel channel, MessageTransportFrame messageFrame) {
        Streamable message = messageFrame.message();
        BiConsumer handler = handlerMap.get(message.getClass());
        if (handler != null) {
            handler.accept(channel, message);
        } else {
            logger.error("handler not found, close channel");
            channel.close();
        }
    }
}
