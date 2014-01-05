package com.hazelcast.disk.core;

import com.hazelcast.nio.serialization.Data;

/**
 * @author: ahmetmircik
 * Date: 12/26/13
 */
public class Bucket {
    private final int depth;
    public static final int NUMBER_OF_RECORDS = 20;
    public static final int SIZE_OF_RECORD = 16 + 1024 + 8;
    public static final int BUCKET_LENGTH = 4 +
            // bucket size info. bits
            4 +
            // key + value sizes
            (Bucket.NUMBER_OF_RECORDS * Bucket.SIZE_OF_RECORD);
    private final Data record;
    private final Data key;

    public Bucket(int depth,Data record, Data key) {
        this.depth = depth;
        this.record = record;
        this.key = key;
    }
}
