package org.mitallast.queue.transport;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.mitallast.queue.Version;
import org.mitallast.queue.action.AbstractAction;
import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.common.Immutable;
import org.mitallast.queue.common.builder.Entry;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.transport.netty.codec.MessageTransportFrame;
import org.mitallast.queue.transport.netty.codec.RequestTransportFrame;

import java.util.function.BiConsumer;

public class TransportController extends AbstractComponent {

    private volatile ImmutableMap<Class, AbstractAction> actionMap = ImmutableMap.of();
    private volatile ImmutableMap<Class, BiConsumer> handlerMap = ImmutableMap.of();

    @Inject
    public TransportController(Settings settings) {
        super(settings);
    }

    public synchronized <Request extends ActionRequest, Response extends ActionResponse>
    void registerHandler(Class requestClass, AbstractAction<Request, Response> action) {
        actionMap = Immutable.compose(actionMap, requestClass, action);
    }

    public synchronized <Message extends Entry>
    void registerMessageHandler(Class requestClass, BiConsumer<TransportChannel, Message> handler) {
        handlerMap = Immutable.compose(handlerMap, requestClass, handler);
    }

    @SuppressWarnings("unchecked")
    public void dispatchMessage(TransportChannel channel, MessageTransportFrame messageFrame) {
        EntryBuilder<Entry> builder = messageFrame.message();
        Entry message = builder.build();
        BiConsumer handler = handlerMap.get(message.getClass());
        if (handler != null) {
            handler.accept(channel, message);
        } else {
            logger.error("handler not found, close channel");
            channel.close();
        }
    }

    @SuppressWarnings("unchecked")
    public void dispatchRequest(TransportChannel channel, RequestTransportFrame requestFrame) {
        EntryBuilder<ActionRequest> builder = requestFrame.message();
        ActionRequest request = builder.build();

        AbstractAction action = actionMap.get(request.getClass());
        if (action != null) {
            action.execute(request).whenComplete((actionResponse, error) -> {
                if (error == null) {
                    RequestTransportFrame response = new RequestTransportFrame(
                            Version.CURRENT,
                            requestFrame.request(),
                            ((ActionResponse) actionResponse).toBuilder());
                    channel.send(response);
                } else {
                    logger.error("unexpected error, close channel", error);
                    channel.close();
                }
            });
        } else {
            logger.error("handler not found, close channel");
            channel.close();
        }
    }
}
