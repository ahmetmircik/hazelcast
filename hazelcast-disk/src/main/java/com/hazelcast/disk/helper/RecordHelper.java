package com.hazelcast.disk.helper;

import com.hazelcast.nio.serialization.Data;

/**
 * @author: ahmetmircik
 * Date: 1/14/14
 */
public enum RecordHelper {
    ;

    public static byte[] asByteArray(Object o) {
        if(o instanceof Data) return ((Data) o).getBuffer();
        return null;
    }
}
