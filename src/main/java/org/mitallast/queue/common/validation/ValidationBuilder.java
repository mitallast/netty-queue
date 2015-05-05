package org.mitallast.queue.common.validation;

import org.mitallast.queue.common.strings.Strings;

public interface ValidationBuilder {
    public static ValidationBuilder EMPTY = new ValidationBuilder() {
        @Override
        public ValidationBuilder missing(String property) {
            return new ValidationException().missing(property);
        }

        @Override
        public ValidationException build() {
            return null;
        }
    };

    public static ValidationBuilder builder() {
        return EMPTY;
    }

    ValidationBuilder missing(String property);

    default ValidationBuilder missing(String property, String value) {
        if (Strings.isEmpty(value)) {
            return missing(property);
        } else {
            return this;
        }
    }

    default <T> ValidationBuilder missing(String property, T value) {
        if (value == null) {
            return missing(property);
        } else {
            return this;
        }
    }

    ValidationException build();
}
