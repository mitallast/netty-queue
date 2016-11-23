package org.mitallast.queue.common;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class Immutable {

    public static <K, V> ImmutableMap<K, V> compose(ImmutableMap<K, V> map, K k, V v) {
        return ImmutableMap.<K, V>builder()
            .putAll(map)
            .put(k, v)
            .build();
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

    public static <V> ImmutableList<V> subtract(Iterable<V> list, V v) {
        ImmutableList.Builder<V> builder = ImmutableList.builder();
        for (V item : list) {
            if (!item.equals(v)) {
                builder.add(item);
            }
        }
        return builder.build();
    }
}
