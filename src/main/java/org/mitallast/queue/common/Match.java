package org.mitallast.queue.common;

import javaslang.collection.HashMap;
import javaslang.collection.Map;

import java.util.function.Function;

public final class Match {

    public static <SuperType, MapType> MapperBuilder<SuperType, MapType> map() {
        return new MapperBuilder<>();
    }

    public static final class Mapper<SuperType, MapType> {
        private final Map<Class, Function<? extends SuperType, MapType>> cases;

        private Mapper(Map<Class, Function<? extends SuperType, MapType>> cases) {
            this.cases = cases;
        }

        @SuppressWarnings({"SuspiciousMethodCalls", "unchecked"})
        public <T extends SuperType> MapType apply(T value) {
            Function<T, MapType> mapper = (Function<T, MapType>) cases.getOrElse(value.getClass(), null);
            if (mapper == null) {
                throw new IllegalArgumentException("Unexpected type: " + value.getClass());
            }
            return mapper.apply(value);
        }
    }

    public static final class MapperBuilder<SuperType, MapType> {

        private Map<Class, Function<? extends SuperType, MapType>> map = HashMap.empty();

        public <Type extends SuperType> MapperBuilder<SuperType, MapType> when(Class<Type> type, Function<Type, MapType> handler) {
            map = map.put(type, handler);
            return this;
        }

        public Mapper<SuperType, MapType> build() {
            return new Mapper<>(map);
        }
    }
}
