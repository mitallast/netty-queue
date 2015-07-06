package org.mitallast.queue.raft.action;

import org.mitallast.queue.action.ActionResponse;
import org.mitallast.queue.raft.RaftError;

public interface RaftResponse<E extends RaftResponse> extends ActionResponse<E> {

    ResponseStatus status();

    RaftError error();
}
