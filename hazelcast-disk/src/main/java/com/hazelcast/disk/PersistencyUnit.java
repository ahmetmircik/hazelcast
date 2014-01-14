package com.hazelcast.disk;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * @author: ahmetmircik
 * Date: 12/20/13
 */
public abstract class PersistencyUnit<K,V> implements Closeable {

    public abstract V put(K key, V value);

    public abstract V get(K key);

    public abstract V remove(K key);

    public abstract void flush();

    public abstract long size();

    public abstract <T> List<T> loadAll() throws IOException;
}
