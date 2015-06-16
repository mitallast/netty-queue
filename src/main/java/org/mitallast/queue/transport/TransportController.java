package org.mitallast.queue.transport;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.mitallast.queue.action.AbstractAction;
import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.concurrent.Listener;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;
import org.mitallast.queue.common.stream.Streams;

import java.io.IOException;

public class TransportController<Request extends ActionRequest, Response extends ActionResponse> extends AbstractComponent {

    private volatile ImmutableMap<Class<Request>, AbstractAction<Request, Response>> actionMap = ImmutableMap.of();

    @Inject
    public TransportController(Settings settings) {
        super(settings);
    }

    public synchronized void registerHandler(Class<Request> requestClass, AbstractAction<Request, Response> action) {
        actionMap = ImmutableMap.<Class<Request>, AbstractAction<Request, Response>>builder()
            .putAll(actionMap)
            .put(requestClass, action)
            .build();
    }

    public void dispatchRequest(TransportChannel channel, TransportFrame requestFrame) throws Exception {
        try (StreamInput streamInput = requestFrame.inputStream()) {
            Class<Request> requestClass = streamInput.readClass();
            Request actionRequest = streamInput.readStreamable(requestClass);
            AbstractAction<Request, Response> action = actionMap.get(requestClass);
            if (action != null) {
                action.execute(actionRequest, new Listener<Response>() {
                    @Override
                    public void onResponse(Response actionResponse) {
                        ByteBuf buffer = Unpooled.buffer();
                        try (StreamOutput streamOutput = Streams.output(buffer)) {
                            actionResponse.writeTo(streamOutput);
                        } catch (Throwable e) {
                            onFailure(e);
                            return;
                        }

                        TransportFrame response = TransportFrame.of(
                            requestFrame.getVersion(),
                            requestFrame.getRequest(),
                            buffer
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
}
