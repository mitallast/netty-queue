package org.mitallast.queue.transport;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.mitallast.queue.action.AbstractAction;
import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.concurrent.Listener;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.transport.netty.codec.StreamableTransportFrame;

import java.io.IOException;

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

    public void dispatchRequest(TransportChannel channel, StreamableTransportFrame requestFrame) throws Exception {
        Request actionRequest = requestFrame.message();
        AbstractAction<Request, Response> action = actionMap.get(actionRequest.getClass());
        if (action != null) {
            action.execute(actionRequest, new Listener<Response>() {
                @Override
                public void onResponse(Response actionResponse) {
                    StreamableTransportFrame response = StreamableTransportFrame.of(
                        requestFrame.version(),
                        requestFrame.request(),
                        actionResponse
                    );
                    channel.send(response);
                }

                @Override
                public void onFailure(Throwable e) {
                    logger.error("error", e);
                }
            });
        } else {
            throw new IOException("Action not found");
        }
    }
}
