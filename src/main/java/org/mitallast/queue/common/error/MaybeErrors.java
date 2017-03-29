package org.mitallast.queue.common.error;


import java.util.function.Function;
import java.util.function.Supplier;

public interface MaybeErrors<T> {

    boolean valid();

    T value();

    Errors errors();

    <R> MaybeErrors<R> map(Function<T, R> mapper);

    T orElse(Supplier<T> producer);

    T orElse(T other);

    static <T> MaybeErrors<T> valid(T value) {
        return new Valid<T>(value);
    }

    static <T> MaybeErrors<T> nonValid(Errors errors) {
        return new NonValid<T>(errors);
    }
}

class Valid<T> implements MaybeErrors<T> {
    private final T value;

    Valid(T value) {
        this.value = value;
    }

    @Override
    public boolean valid() {
        return true;
    }

    @Override
    public T value() {
        return value;
    }

    @Override
    public Errors errors() {
        throw new IllegalStateException();
    }

    @Override
    public <R> MaybeErrors<R> map(Function<T, R> mapper) {
        return new Valid<R>(mapper.apply(value));
    }

    @Override
    public T orElse(Supplier<T> producer) {
        return producer.get();
    }

    @Override
    public T orElse(T other) {
        return other;
    }
}

class NonValid<T> implements MaybeErrors<T> {
    private final Errors errors;

    NonValid(Errors errors) {
        this.errors = errors;
    }

    @Override
    public boolean valid() {
        return false;
    }

    @Override
    public T value() {
        throw new IllegalStateException();
    }

    @Override
    public Errors errors() {
        return errors;
    }

    @Override
    public <R> MaybeErrors<R> map(Function<T, R> mapper) {
        return new NonValid<R>(errors);
    }

    @Override
    public T orElse(Supplier<T> producer) {
        return producer.get();
    }

    @Override
    public T orElse(T other) {
        return other;
    }
}