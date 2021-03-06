package com.sc.zhaoqi.common.sync;

import java.util.AbstractMap;
import java.util.Map;

/**
 * @author sczhaoqi
 */
public class Entrys {

    public static <K, V> Map.Entry<K, V> newEntry(K key, V value) {
        return new AbstractMap.SimpleEntry<>(key, value);
    }

    public static <K, V> Map.Entry<K, V> newImmutableEntry(K key, V value) {
        return new AbstractMap.SimpleImmutableEntry<>(key, value);
    }
}
