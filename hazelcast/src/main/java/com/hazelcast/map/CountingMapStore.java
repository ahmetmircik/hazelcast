package com.hazelcast.map;

import com.hazelcast.core.MapStore;

import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A map store which counts the different operations.
 * <p>
 * Furthermore it can be configured to throw exceptions in store/storeAll/delete/deleteAll.
 */
public class CountingMapStore<V> implements MapStore<String, V> {

    // ---------------------------------------------------------------- counters

    public AtomicInteger countLoadAllKeys = new AtomicInteger();

    public AtomicInteger countLoad = new AtomicInteger();

    public AtomicInteger countLoadAll = new AtomicInteger();

    public AtomicInteger countStore = new AtomicInteger();

    public AtomicInteger countStoreAll = new AtomicInteger();

    public AtomicInteger countDelete = new AtomicInteger();

    public AtomicInteger countDeleteAll = new AtomicInteger();

    public AtomicInteger countNumberStore = new AtomicInteger();

    public AtomicInteger countNumberDelete = new AtomicInteger();

    // ---------------------------------------------------------------- members

    private ConcurrentHashMap<String, V> store = new ConcurrentHashMap<String, V>();

    private int numExceptionsInStore;

    private int numExceptionsInDelete;

    private boolean exceptionInStoreAll;

    private boolean exceptionInDeleteAll;

    // ----------------------------------------------------------- construction

    public CountingMapStore() {
        this.exceptionInStoreAll = false;
        this.exceptionInDeleteAll = false;
    }

    public CountingMapStore(boolean exceptionInStoreAll, boolean exceptionInDeleteAll) {
        this.exceptionInStoreAll = exceptionInStoreAll;
        this.exceptionInDeleteAll = exceptionInDeleteAll;
    }

    public CountingMapStore(int numExceptionsInStore, int numExceptionsInDelete) {
        this.numExceptionsInStore = numExceptionsInStore;
        this.numExceptionsInDelete = numExceptionsInDelete;
        this.exceptionInStoreAll = true;
        this.exceptionInDeleteAll = true;
    }

    // ----------------------------------------------------- MapStore interface

    @Override
    public Set<String> loadAllKeys() {
        countLoadAllKeys.incrementAndGet();
        Enumeration<String> keys = store.keys();
        HashSet<String> strings = new HashSet<String>();
        while (keys.hasMoreElements()) {
            strings.add(keys.nextElement());
        }
        return strings;
    }

    @Override
    public V load(String key) {
        countLoad.incrementAndGet();
        return store.get(key);
    }

    @Override
    public Map<String, V> loadAll(Collection<String> keys) {
        countLoadAll.incrementAndGet();
        Map<String, V> result = new HashMap<String, V>();
        for (String key : keys) {
            V value = store.get(key);
            if (value != null) {
                result.put(key, value);
            }
        }
        return result;
    }

    @Override
    public void store(String key, V value) {
        countStore.incrementAndGet();
        countNumberStore.incrementAndGet();
        if (numExceptionsInStore > 0) {
            numExceptionsInStore--;
            throw new RuntimeException("Exception in store().");
        }
        store.put(key, value);
    }

    @Override
    public void storeAll(Map<String, V> map) {
        countStoreAll.incrementAndGet();
        countNumberStore.addAndGet(map.size());
        store.putAll(map);
        if (exceptionInStoreAll) {
            throw new RuntimeException("Exception in storeAll().");
        }
    }

    @Override
    public void delete(String key) {
        countDelete.incrementAndGet();
        countNumberDelete.incrementAndGet();
        if (numExceptionsInDelete > 0) {
            numExceptionsInDelete--;
            throw new RuntimeException("Exception in delete().");
        }
        store.remove(key);
    }

    @Override
    public void deleteAll(Collection<String> keys) {
        countDeleteAll.incrementAndGet();
        countNumberDelete.addAndGet(keys.size());
        for (String key : keys) {
            store.remove(key);
        }
        if (exceptionInDeleteAll) {
            throw new RuntimeException("Exception in deleteAll().");
        }
    }

    /**
     * Get number of entries in store.
     */
    public int size() {
        return store.size();
    }

    // ---------------------------------------------------------------- helpers

    public void printCounts(String title) {
        StringBuilder buf = new StringBuilder();
        buf.append(title + ":\n");
        buf.append("- num load all keys = " + countLoadAllKeys.get() + "\n");
        buf.append("- num load          = " + countLoad.get() + "\n");
        buf.append("- num load all      = " + countLoadAll.get() + "\n");
        buf.append("- num store         = " + countStore.get() + "\n");
        buf.append("- num store all     = " + countStoreAll.get() + "\n");
        buf.append("- num delete        = " + countDelete.get() + "\n");
        buf.append("- num delete all    = " + countDeleteAll.get() + "\n");
        buf.append("- count store       = " + countNumberStore.get() + "\n");
        buf.append("- count delete      = " + countNumberDelete.get() + "\n");
        System.out.println(buf.toString());
    }

}

