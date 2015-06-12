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
import org.mitallast.queue.common.stream.ByteBufStreamOutput;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;

import java.io.IOException;

public class TransportController extends AbstractComponent {

    private volatile ImmutableMap<String, AbstractAction> actionMap = ImmutableMap.of();

    @Inject
    public TransportController(Settings settings) {
        super(settings);
    }

    public synchronized void registerHandler(String actionName, AbstractAction action) {
        actionMap = ImmutableMap.<String, AbstractAction>builder()
            .putAll(actionMap)
            .put(actionName, action)
            .build();
    }

    @SuppressWarnings("unchecked")
    public void dispatchRequest(TransportChannel channel, TransportFrame request) throws Exception {
        try (StreamInput streamInput = request.inputStream()) {
            String actionName = streamInput.readText();
            AbstractAction<ActionRequest, ActionResponse> action = actionMap.get(actionName);
            if (action != null) {
                ActionRequest actionRequest = action.createRequest();
                actionRequest.readFrom(streamInput);
                action.execute(actionRequest, new Listener<ActionResponse>() {
                    @Override
                    public void onResponse(ActionResponse actionResponse) {
                        ByteBuf buffer = Unpooled.buffer();
                        try (StreamOutput streamOutput = new ByteBufStreamOutput(buffer)) {
                            actionResponse.writeTo(streamOutput);

                        } catch (Throwable e) {
                            onFailure(e);
                            return;
                        }

                        TransportFrame response = TransportFrame.of(
                            request.getVersion(),
                            request.getRequest(),
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
