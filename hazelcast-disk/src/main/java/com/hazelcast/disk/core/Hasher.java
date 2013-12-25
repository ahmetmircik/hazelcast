package com.hazelcast.disk.core;

/**
 * @author: ahmetmircik
 * Date: 12/23/13
 */
public interface Hasher<Key,Hash> {
     Hash hash(Key key);
}
