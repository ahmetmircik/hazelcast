package com.hazelcast.disk.core;

import java.nio.MappedByteBuffer;

/**
 * @author: ahmetmircik
 * Date: 12/30/13
 */
public interface Mappable {

    void write(MappedByteBuffer mappedByteBuffer);
}
