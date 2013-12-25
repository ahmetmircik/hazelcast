package com.hazelcast.disk.impl;

import com.hazelcast.disk.Storage;

import java.io.IOException;

/**
 * @author: ahmetmircik
 * Date: 12/20/13
 */
public final class Storages {
    private Storages(){ }

    public static <K,V> Storage<K,V> createMMap(String name, long position, long size) throws IOException {
        final StorageConfig storageConfig = new StorageConfig() {
            @Override
            public long getBlockSize() {
                return 4 * 1024;
            }

            @Override
            public String getMode() {
                return "rw";
            }
        };
        final MMappedFile<K,V> mMappedFile = new MMappedFile<K,V>(name, storageConfig);
        mMappedFile.createNew();
        return mMappedFile;

    }

}
