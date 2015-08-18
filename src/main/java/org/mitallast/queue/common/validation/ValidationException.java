package org.mitallast.queue.common.validation;

import java.util.ArrayList;
import java.util.List;

public class ValidationException extends RuntimeException implements ValidationBuilder {

    private final List<String> validationErrors = new ArrayList<>();

    public ValidationException() {
        super();
    }

    public void addValidationError(String error) {
        validationErrors.add(error);
    }

    @Override
    public ValidationBuilder missing(String property) {
        addValidationError(property + " is missing");
        return this;
    }

    @Override
    public ValidationException build() {
        return this;
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
