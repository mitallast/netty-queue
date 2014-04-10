package org.mitallast.queue.action;

import org.mitallast.queue.QueueException;

import java.util.ArrayList;
import java.util.List;

public class ActionRequestValidationException extends QueueException {

    private final List<String> validationErrors = new ArrayList<>();

    public ActionRequestValidationException() {
        super();
    }

    public void addValidationError(String error) {
        validationErrors.add(error);
    }

    public void addValidationErrors(Iterable<String> errors) {
        for (String error : errors) {
            validationErrors.add(error);
        }
    }

    @Override
    public String getMessage() {
        StringBuilder sb = new StringBuilder();
        sb.append("Validation Failed: ");
        int index = 0;
        for (String error : validationErrors) {
            sb.append(++index).append(": ").append(error).append(";");
        }
        return sb.toString();
    }
}
