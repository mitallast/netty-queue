package org.mitallast.queue.transport;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.mitallast.queue.action.AbstractAction;
import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.common.builder.EntryBuilder;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.concurrent.Futures;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.transport.netty.codec.StreamableTransportFrame;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class TransportController<Request extends ActionRequest, Response extends ActionResponse> extends AbstractComponent {

    private volatile ImmutableMap<Class, AbstractAction<Request, Response>> actionMap = ImmutableMap.of();

    @Inject
    public TransportController(Settings settings) {
        super(settings);
    }

    public synchronized void registerHandler(Class requestClass, AbstractAction<Request, Response> action) {
        actionMap = ImmutableMap.<Class, AbstractAction<Request, Response>>builder()
            .putAll(actionMap)
            .put(requestClass, action)
            .build();
    }

    public void dispatchRequest(TransportChannel channel, StreamableTransportFrame requestFrame) {
        EntryBuilder<? extends EntryBuilder, Request> builder = requestFrame.message();
        Request actionRequest = builder.build();
        dispatchRequest(actionRequest).whenComplete((actionResponse, error) -> {
            if (error == null) {
                StreamableTransportFrame response = StreamableTransportFrame.of(
                    requestFrame.version(),
                    requestFrame.request(),
                    actionResponse.toBuilder()
                );
                channel.send(response);
            } else {
                logger.error("error", error);
                channel.close();
            }
        });
    }

    public CompletableFuture<Response> dispatchRequest(Request request) {
        AbstractAction<Request, Response> action = actionMap.get(request.getClass());
        if (action != null) {
            return action.execute(request);
        } else {
            return Futures.completeExceptionally(new IOException("Action not found"));
        }
    }
}
