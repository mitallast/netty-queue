package org.mitallast.queue.action;

public abstract class ActionRequest {
    public abstract ActionRequestValidationException validate();
}
