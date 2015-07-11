package org.mitallast.queue.action;

public class ActionResponseError extends RuntimeException {
    public ActionResponseError(String message) {
        super(message);
    }
}
