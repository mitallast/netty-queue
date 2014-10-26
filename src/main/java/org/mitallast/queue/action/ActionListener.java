package org.mitallast.queue.action;

public interface ActionListener<Response extends ActionResponse> {
    /**
     * A response handler.
     */
    void onResponse(Response response);

    /**
     * A failure handler.
     */
    void onFailure(Throwable e);
}
