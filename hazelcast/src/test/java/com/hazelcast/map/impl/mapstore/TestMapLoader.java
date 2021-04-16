package com.hazelcast.map.impl.mapstore;

import com.hazelcast.map.MapLoader;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TestMapLoader implements MapLoader<Integer, Integer> {

    static final int DEFAULT_SIZE = 1000;

    final Map<Integer, Integer> map = new ConcurrentHashMap<>(DEFAULT_SIZE);

    public TestMapLoader() {
        this(DEFAULT_SIZE);
    }

    public TestMapLoader(int size) {
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
    }

    @Override
    public Integer load(Integer key) {
        return map.get(key);
    }

    @Override
    public Map<Integer, Integer> loadAll(Collection<Integer> keys) {
        HashMap<Integer, Integer> hashMap = new HashMap<>();
        for (Integer key : keys) {
            hashMap.put(key, map.get(key));
        }
        return hashMap;
    }

    @Override
    public Iterable<Integer> loadAllKeys() {
        System.err.println("TestMapLoader.loadAllKeys");
        return map.keySet();
    }
}