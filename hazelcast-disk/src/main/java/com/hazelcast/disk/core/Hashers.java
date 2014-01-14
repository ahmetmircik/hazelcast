package com.hazelcast.disk.core;

/**
 * @author: ahmetmircik
 * Date: 1/14/14
 */
public enum Hashers {
         ;

    private static final Hasher DATA_HASHER = new Hasher.DataHasher();

    public static final Integer  getHasher(byte[] data) {
        return (Integer) DATA_HASHER.hash(data);
    }
}
