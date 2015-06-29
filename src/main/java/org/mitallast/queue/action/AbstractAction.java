package org.mitallast.queue.action;

import org.mitallast.queue.common.component.AbstractComponent;
import org.mitallast.queue.common.concurrent.Futures;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.common.validation.ValidationException;
import org.mitallast.queue.transport.TransportController;

import java.lang.reflect.ParameterizedType;
import java.util.concurrent.CompletableFuture;

public abstract class AbstractAction<Request extends ActionRequest, Response extends ActionResponse> extends AbstractComponent {

    @SuppressWarnings("all")
    public AbstractAction(Settings settings, TransportController controller) {
        super(settings);
        Class<Request> requestClass = (Class<Request>) ((ParameterizedType) getClass().getGenericSuperclass())
            .getActualTypeArguments()[0];
        controller.registerHandler(requestClass, this);
    }

    public CompletableFuture<Response> execute(Request request) {
        CompletableFuture<Response> future = Futures.future();
        execute(request, future);
        return future;
    }

    public void execute(Request request, CompletableFuture<Response> listener) {
        ValidationException validationException = request.validate().build();
        if (validationException != null) {
            listener.completeExceptionally(validationException);
        } else {
            executeInternal(request, listener);
        }
    }

    protected abstract void executeInternal(Request request, CompletableFuture<Response> listener);
}
