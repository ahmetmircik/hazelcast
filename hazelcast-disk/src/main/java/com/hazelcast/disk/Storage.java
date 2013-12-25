package com.hazelcast.disk;

/**
 * @author: ahmetmircik
 * Date: 12/19/13
 */
public interface Storage<K,V> {

    void put(K key, V value);

    V get(K key);

    V remove(K key);

    void close();

    long getPosition();

    long getSize();

}
