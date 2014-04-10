package org.mitallast.queue.action;

public interface ActionListener<Response> {
    /**
     * A response handler.
     */
    void onResponse(Response response);

    /**
     * A failure handler.
     */
    void onFailure(Throwable e);
}
