package org.mitallast.queue.common.error;

import com.google.common.collect.ImmutableMap;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public final class Errors {

    private HashMap<String, String> errors;
    private HashMap<String, Errors> nested;

    public Map<String, String> errors() {
        if (errors == null) {
            return ImmutableMap.of();
        } else {
            return errors;
        }
    }

    public Map<String, Errors> nested() {
        if (nested == null) {
            return ImmutableMap.of();
        } else {
            return nested;
        }
    }

    public BiConsumer<String, String> on(boolean expression) {
        if (expression) {
            return this::error;
        } else {
            return (key, message) -> {
            };
        }
    }

    public BiConsumer<String, String> not(boolean expression) {
        return on(!expression);
    }

    public BiConsumer<String, String> notNull(Object value) {
        return not(value != null);
    }

    public BiConsumer<String, String> required(String value) {
        return on(value == null || value.isEmpty());
    }

    public BiConsumer<String, String> required(Collection value) {
        return on(value == null || value.isEmpty());
    }

    public void error(String key, String message) {
        if (errors == null) {
            errors = new HashMap<>();
        }
        errors.put(key, message);
    }

    public Errors builder(String key) {
        if (nested == null) {
            nested = new HashMap<>();
        }
        return nested.computeIfAbsent(key, (k) -> new Errors());
    }

    public Errors builder(int key) {
        return builder(String.valueOf(key));
    }

    public boolean valid() {
        return (errors == null || errors.isEmpty())
            && (nested == null || nested.values().stream().map(Errors::valid).reduce(true, (a, b) -> a && b));
    }

    public <T> MaybeErrors<T> maybe(Supplier<T> supplier) {
        if (valid()) {
            return MaybeErrors.valid(supplier.get());
        } else {
            return MaybeErrors.nonValid(this);
        }
    }

    @Override
    public String toString() {
        return "Errors{" +
            "errors=" + errors +
            ", nested=" + nested +
            '}';
    }
}
