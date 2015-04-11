package org.mitallast.queue.action;

import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.concurrent.Listener;
import org.mitallast.queue.common.concurrent.futures.Futures;
import org.mitallast.queue.common.concurrent.futures.ListenerSmartFuture;
import org.mitallast.queue.common.concurrent.futures.SmartFuture;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.transport.transport.TransportController;

public abstract class AbstractAction<Request extends ActionRequest, Response extends ActionResponse> extends AbstractComponent {

    public AbstractAction(Settings settings, TransportController controller) {
        super(settings);
        controller.registerHandler(this);
    }

    public SmartFuture<Response> execute(Request request) {
        ListenerSmartFuture<Response> listener = Futures.listenerFuture();
        execute(request, listener);
        return listener;
    }

    public abstract void execute(Request request, Listener<Response> listener);

    public abstract ActionType getActionId();

    public abstract Request createRequest();
}
