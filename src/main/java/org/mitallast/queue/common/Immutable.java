package org.mitallast.queue.common;

import com.google.common.base.Preconditions;
import com.google.common.collect.*;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigValue;

import java.util.*;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;

public final class Immutable {
    private Immutable() {
    }

    public static <T> Optional<T> headOpt(List<T> values) {
        if (values.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(values.get(0));
        }
    }

    public static <K, V> ImmutableListMultimap<K, V> groupList(Collection<V> values, Function<V, K> mapper) {
        ImmutableListMultimap.Builder<K, V> builder = ImmutableListMultimap.builder();
        for (V value : values) {
            builder.put(mapper.apply(value), value);
        }
        return builder.build();
    }

    public static <K, V extends Comparable<V>> ImmutableListMultimap<K, V> groupSorted(Collection<V> values,
                                                                                       Function<V, K> mapper) {
        ImmutableListMultimap.Builder<K, V> builder = ImmutableListMultimap.builder();
        builder.orderValuesBy(Comparator.naturalOrder());
        for (V value : values) {
            builder.put(mapper.apply(value), value);
        }
        return builder.build();
    }

    public static <K, V> ImmutableMap<K, V> group(Collection<V> values, Function<V, K> mapper) {
        ImmutableMap.Builder<K, V> builder = ImmutableMap.builder();
        for (V value : values) {
            builder.put(mapper.apply(value), value);
        }
        return builder.build();
    }

    public static <K, V> ImmutableMap<K, V> reduce(ImmutableListMultimap<K, V> multimap,
                                                   Function<ImmutableCollection<V>, V> reducer) {
        ImmutableMap.Builder<K, V> builder = new ImmutableMap.Builder<>();
        for (K key : multimap.keySet()) {
            builder.put(key, reducer.apply(multimap.get(key)));
        }
        return builder.build();
    }

    public static <K, V, T> ImmutableListMultimap<K, T> map(ImmutableListMultimap<K, V> multimap,
                                                            Function<ImmutableCollection<V>, Iterable<T>> mapper) {
        ImmutableListMultimap.Builder<K, T> builder = ImmutableListMultimap.builder();
        for (K key : multimap.keySet()) {
            builder.putAll(key, mapper.apply(multimap.get(key)));
        }
        return builder.build();
    }

    public static <V, T> ImmutableList<T> map(ImmutableList<V> list, Function<V, T> mapper) {
        ImmutableList.Builder<T> builder = ImmutableList.builder();
        for (V v : list) {
            builder.add(mapper.apply(v));
        }
        return builder.build();
    }

    public static <V, T> ImmutableList<T> flatMap(Collection<V> list, Function<V, Iterable<T>> mapper) {
        ImmutableList.Builder<T> builder = ImmutableList.builder();
        for (V v : list) {
            builder.addAll(mapper.apply(v));
        }
        return builder.build();
    }

    public static <T> ImmutableList<T> sort(Collection<T> values, Comparator<? super T> comparator) {
        ArrayList<T> arrayList = new ArrayList<>(values);
        arrayList.sort(comparator);
        return ImmutableList.copyOf(arrayList);
    }

    public static <T extends Comparable<T>> ImmutableList<T> sort(Collection<T> values) {
        return sort(values, Comparator.naturalOrder());
    }

    public static <T extends Comparable<T>> ImmutableList<T> reverseSort(Collection<T> values) {
        return sort(values, Comparator.reverseOrder());
    }

    public static <T> T last(ImmutableCollection<T> values) {
        Preconditions.checkArgument(!values.isEmpty(), "Cannot be empty");
        return values.asList().get(values.size() - 1);
    }

    public static <T> ImmutableList<T> replace(ImmutableList<T> list, Predicate<T> match, T value) {
        ImmutableList.Builder<T> builder = ImmutableList.builder();
        for (T t : list) {
            if (match.test(t)) {
                builder.add(value);
            } else {
                builder.add(t);
            }
        }
        return builder.build();
    }

    public static <T> ImmutableList<T> append(ImmutableList<T> list, T... values) {
        ImmutableList.Builder<T> builder = ImmutableList.builder();
        builder.addAll(list).add(values);
        return builder.build();
    }

    public static ImmutableMap<String, String> toMap(Config config) {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for (Map.Entry<String, ConfigValue> entry : config.entrySet()) {
            switch (entry.getValue().valueType()) {
                case BOOLEAN:
                case NUMBER:
                case STRING:
                    String value = entry.getValue().unwrapped().toString();
                    builder.put(entry.getKey(), value);
                    break;
                default:
                    throw new IllegalArgumentException("Unexpected entry type");
            }
        }
        return builder.build();
    }

    public static <K, V> ImmutableMap<K, V> compose(ImmutableMap<K, V> map, K k, V v) {
        ImmutableMap.Builder<K, V> builder = ImmutableMap.builder();
        map.forEach((key, value) -> {
            if (!key.equals(k)) {
                builder.put(key, value);
            }
        });
        builder.put(k, v);
        return builder.build();
    }

    public static <K, V> ImmutableMap<K, V> subtract(ImmutableMap<K, V> map, K k) {
        ImmutableMap.Builder<K, V> builder = ImmutableMap.builder();
        map.entrySet().stream()
            .filter(kvEntry -> !kvEntry.getKey().equals(k))
            .forEach(builder::put);
        return builder.build();
    }

    public static <K, V> ImmutableMap<K, V> replace(ImmutableMap<K, V> map, K k, V v) {
        ImmutableMap.Builder<K, V> builder = ImmutableMap.builder();
        map.entrySet().stream()
            .filter(kvEntry -> !kvEntry.getKey().equals(k))
            .forEach(builder::put);
        builder.put(k, v);
        return builder.build();
    }

    public static <V> ImmutableList<V> compose(Iterable<V> list, V v) {
        return ImmutableList.<V>builder()
            .addAll(list)
            .add(v)
            .build();
    }

    public static <V> ImmutableSet<V> compose(ImmutableSet<V> set, V v) {
        return ImmutableSet.<V>builder()
            .addAll(set)
            .add(v)
            .build();
    }

    public static <V> ImmutableList<V> replace(ImmutableList<V> list, int index, V value) {
        ImmutableList.Builder<V> builder = ImmutableList.builder();
        for (int i = 0; i < list.size(); i++) {
            final V item;
            if (i == index) {
                item = value;
            } else {
                item = list.get(i);
            }
            builder.add(item);
        }
        return builder.build();
    }

    public static <V> ImmutableList<V> subtract(ImmutableList<V> list, V v) {
        return filterNot(list, item -> item.equals(v));
    }

    public static <V> ImmutableList<V> filterNot(ImmutableList<V> list, Predicate<V> predicate) {
        ImmutableList.Builder<V> builder = ImmutableList.builder();
        for (V item : list) {
            if (!predicate.test(item)) {
                builder.add(item);
            }
        }
        return builder.build();
    }

    public static <V> ImmutableSet<V> subtract(ImmutableSet<V> set, V value) {
        return filterNot(set, item -> item.equals(value));
    }

    public static <V> ImmutableSet<V> filterNot(ImmutableSet<V> list, Predicate<V> predicate) {
        ImmutableSet.Builder<V> builder = ImmutableSet.builder();
        for (V item : list) {
            if (!predicate.test(item)) {
                builder.add(item);
            }
        }
        return builder.build();
    }

    public static ImmutableMultimap<String, String> toMultimap(Config config) {
        ImmutableMultimap.Builder<String, String> builder = ImmutableMultimap.builder();
        for (Map.Entry<String, ConfigValue> entry : config.entrySet()) {
            switch (entry.getValue().valueType()) {
                case BOOLEAN:
                case NUMBER:
                case STRING:
                    String value = entry.getValue().unwrapped().toString();
                    builder.put(entry.getKey(), value);
                    break;
                case LIST:
                    ConfigList list = ((ConfigList) entry.getValue());
                    for (ConfigValue configValue : list) {
                        switch (configValue.valueType()) {
                            case BOOLEAN:
                            case NUMBER:
                            case STRING:
                                String listValue = entry.getValue().unwrapped().toString();
                                builder.put(entry.getKey(), listValue);
                                break;
                            default:
                                throw new IllegalArgumentException("Unexpected entry type");
                        }
                    }

                default:
                    throw new IllegalArgumentException("Unexpected entry type");
            }
        }
        return builder.build();
    }

    public static <K, V> ImmutableMultimap<K, V> compose(ImmutableMultimap<K, V> multimap, K key, V value) {
        ImmutableMultimap.Builder<K, V> builder = ImmutableMultimap.builder();
        for (Map.Entry<K, V> entry : multimap.entries()) {
            builder.put(entry.getKey(), entry.getValue());
        }
        builder.put(key, value);
        return builder.build();
    }

    public static <K, V> ImmutableMultimap<K, V> subtract(ImmutableMultimap<K, V> multimap, K key, V value) {
        ImmutableMultimap.Builder<K, V> builder = ImmutableMultimap.builder();
        for (Map.Entry<K, V> entry : multimap.entries()) {
            if (!entry.getKey().equals(key) || !entry.getValue().equals(value)) {
                builder.put(entry.getKey(), entry.getValue());
            }
        }
        return builder.build();
    }

    public static <V> ImmutableList<V> generate(int max, IntFunction<V> generator) {
        ImmutableList.Builder<V> builder = ImmutableList.builder();
        for (int i = 0; i < max; i++) {
            builder.add(generator.apply(i));
        }
        return builder.build();
    }
}
