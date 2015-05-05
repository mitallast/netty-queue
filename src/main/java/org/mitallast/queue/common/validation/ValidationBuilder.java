package org.mitallast.queue.common.validation;

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

    ValidationException build();
}
