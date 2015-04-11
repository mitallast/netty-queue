package org.mitallast.queue.transport.transport;

import com.google.inject.Inject;
import io.netty.buffer.ByteBuf;
import org.mitallast.queue.action.AbstractAction;
import org.mitallast.queue.action.ActionRequest;
import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.common.collection.ImmutableIntMap;
import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.concurrent.Listener;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.common.stream.ByteBufStreamOutput;
import org.mitallast.queue.common.stream.StreamInput;
import org.mitallast.queue.common.stream.StreamOutput;

public class TransportController extends AbstractComponent {

    private volatile ImmutableIntMap<AbstractAction> actionMap = ImmutableIntMap.empty();

    @Inject
    public TransportController(Settings settings) {
        super(settings);
    }

    public synchronized void registerHandler(AbstractAction action) {
        actionMap = ImmutableIntMap.<AbstractAction>builder()
            .putAll(actionMap)
            .put(action.getActionId().id(), action)
            .build();
    }

    @SuppressWarnings("unchecked")
    public void dispatchRequest(TransportChannel channel, TransportFrame request) throws Exception {
        try (StreamInput streamInput = request.inputStream()) {
            int actionId = streamInput.readInt();
            AbstractAction<ActionRequest, ActionResponse> action = actionMap.get(actionId);
            if (action != null) {
                ActionRequest actionRequest = action.createRequest();
                actionRequest.readFrom(streamInput);
                action.execute(actionRequest, new Listener<ActionResponse>() {
                    @Override
                    public void onResponse(ActionResponse queueMessage) {
                        ByteBuf buffer = channel.ctx().alloc().ioBuffer();
                        try (StreamOutput streamOutput = new ByteBufStreamOutput(buffer)) {
                            queueMessage.writeTo(streamOutput);

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
            }
        }
    }
}
