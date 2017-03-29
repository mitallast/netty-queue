package org.mitallast.queue.common;

import com.google.common.collect.ImmutableMap;

import java.io.IOException;

public final class Match {

    public static <SuperType, MapType> MapperBuilder<SuperType, MapType> map() {
        return new MapperBuilder<>();
    }

    public static <SuperType> HandlerBuilder<SuperType> handle() {
        return new HandlerBuilder<>();
    }

    @FunctionalInterface
    public interface MapperFunction<T, R> {
        R apply(T value) throws IOException;
    }

    public static final class Mapper<SuperType, MapType> {
        private final ImmutableMap<Class<? extends SuperType>, MapperFunction<? extends SuperType, MapType>> cases;

        private Mapper(ImmutableMap<Class<? extends SuperType>, MapperFunction<? extends SuperType, MapType>> cases) {
            this.cases = cases;
        }

        @SuppressWarnings({"SuspiciousMethodCalls", "unchecked"})
        public <T extends SuperType> MapType apply(T value) throws IOException {
            MapperFunction<T, MapType> mapper = (MapperFunction<T, MapType>) cases.get(value.getClass());
            if (mapper == null) {
                throw new IllegalArgumentException("Unexpected type: " + value.getClass());
            }
            return mapper.apply(value);
        }
    }

    public static final class MapperBuilder<SuperType, MapType> {

        private final ImmutableMap.Builder<Class<? extends SuperType>, MapperFunction<? extends SuperType, MapType>> builder = ImmutableMap.builder();

        public <Type extends SuperType> MapperBuilder<SuperType, MapType> when(Class<Type> type, MapperFunction<Type, MapType> handler) {
            builder.put(type, handler);
            return this;
        }

        public Mapper<SuperType, MapType> build() {
            return new Mapper<>(builder.build());
        }
    }

    @FunctionalInterface
    public interface HandlerFunction<T> {
        void accept(T value) throws IOException;
    }

    public static final class Handler<SuperType> {
        private final ImmutableMap<Class<? extends SuperType>, HandlerFunction<? extends SuperType>> cases;

        private Handler(ImmutableMap<Class<? extends SuperType>, HandlerFunction<? extends SuperType>> cases) {
            this.cases = cases;
        }

        @SuppressWarnings({"SuspiciousMethodCalls", "unchecked"})
        public <T extends SuperType> void accept(T value) throws IOException {
            HandlerFunction<T> handler = (HandlerFunction<T>) cases.get(value.getClass());
            if (handler == null) {
                throw new IllegalArgumentException("Unexpected type: " + value.getClass());
            }
            handler.accept(value);
        }
    }

    public static final class HandlerBuilder<SuperType> {

        private final ImmutableMap.Builder<Class<? extends SuperType>, HandlerFunction<? extends SuperType>> builder =
            ImmutableMap.builder();

        public <Type extends SuperType> HandlerBuilder<SuperType> when(Class<Type> type, HandlerFunction<Type> handler) {
            builder.put(type, handler);
            return this;
        }

        public Handler<SuperType> build() {
            return new Handler<>(builder.build());
        }
    }
}
