package org.mitallast.queue.action;

import org.mitallast.queue.common.module.AbstractComponent;
import org.mitallast.queue.common.settings.Settings;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractAction<Request extends ActionRequest, Response extends ActionResponse> extends AbstractComponent {

    protected final ExecutorService executorService;

    public AbstractAction(Settings settings, ExecutorService executorService) {
        super(settings);
        this.executorService = executorService;
    }

    public Future<Response> execute(final Request request) {
        return executorService.submit(new Callable<Response>() {

            @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
            @Override
            public Response call() throws Exception {
                final AtomicReference<Response> responseReference = new AtomicReference<>(null);
                final AtomicReference<Throwable> throwableReference = new AtomicReference<>(null);

                doExecute(request, new ActionListener<Response>() {
                    @Override
                    public void onResponse(Response response) {
                        responseReference.set(response);
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        throwableReference.set(e);
                    }
                });
                if (throwableReference.get() != null) {
                    throw (Exception) throwableReference.get();
                }
                return responseReference.get();
            }
        });
    }

    public void execute(final Request request, final ActionListener<Response> listener) {
        doExecute(request, listener);
    }

    protected abstract void doExecute(Request request, ActionListener<Response> listener);
}
