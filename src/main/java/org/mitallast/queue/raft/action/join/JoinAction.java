package org.mitallast.queue.raft.action.join;

import com.google.inject.Inject;
import org.mitallast.queue.action.AbstractAction;
import org.mitallast.queue.common.settings.Settings;
import org.mitallast.queue.raft.state.RaftStateContext;
import org.mitallast.queue.raft.util.ExecutionContext;
import org.mitallast.queue.transport.TransportController;

import java.util.concurrent.CompletableFuture;

public class JoinAction extends AbstractAction<JoinRequest, JoinResponse> {

    private final RaftStateContext context;
    private final ExecutionContext executionContext;

    @Inject
    public JoinAction(Settings settings, TransportController controller, RaftStateContext context, ExecutionContext executionContext) {
        super(settings, controller);
        this.context = context;
        this.executionContext = executionContext;
    }

    @Override
    protected void executeInternal(JoinRequest request, CompletableFuture<JoinResponse> listener) {
        executionContext.execute("join request handler", () -> {
            context.raftState().join(request).whenComplete((response, error) -> {
                if (error == null) {
                    listener.complete(response);
                } else {
                    listener.completeExceptionally(error);
                }
            });
        });
    }
}
