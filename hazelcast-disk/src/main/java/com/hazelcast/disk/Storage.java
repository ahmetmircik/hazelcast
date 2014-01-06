package com.hazelcast.disk;

import java.io.Closeable;

/**
 * @author: ahmetmircik
 * Date: 12/19/13
 */
public interface Storage extends Closeable{

    int getInt(long offset);

    long getLong(long offset);

    void getBytes(long offset, byte[] value);

    void writeInt(long offset,int value);

    void writeLong(long offset,long value);

    void writeBytes(long offset, byte[] value);
}
