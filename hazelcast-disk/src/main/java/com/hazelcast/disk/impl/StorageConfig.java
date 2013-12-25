package com.hazelcast.disk.impl;

/**
 * @author: ahmetmircik
 * Date: 12/23/13
 */
public interface StorageConfig {
    long getBlockSize();
    String getMode();
}
