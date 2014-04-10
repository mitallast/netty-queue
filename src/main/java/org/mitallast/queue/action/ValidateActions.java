package org.mitallast.queue.action;

public class ValidateActions {

    public static ActionRequestValidationException addValidationError(String error, ActionRequestValidationException validationException) {
        if (validationException == null) {
            validationException = new ActionRequestValidationException();
        }
        validationException.addValidationError(error);
        return validationException;
    }
}
