package com.hazelcast.disk.core;

import com.hazelcast.disk.Storage;
import com.hazelcast.nio.serialization.Data;

import java.io.IOError;
import java.io.IOException;
import java.util.HashSet;

/**
 * @author: ahmetmircik
 * Date: 12/27/13
 */
public class ReadIndexFile {

    private static final Hasher<Data, Integer> HASHER = Hasher.DATA_HASHER;

    private String path;

    private Storage index;
    private Storage data;

    int globalDepth;

    int count;

    public ReadIndexFile(String path) {
        this.path = path;
        index = new MappedView(this.path + ".index", HashTable.INDEX_BLOCK_LENGTH);
        data = new MappedView(this.path + ".data", HashTable.DATA_BLOCK_LENGTH);
        init();
    }

    void init() {
        globalDepth = index.getInt(0L);
        count = index.getInt(4L);
        System.out.println("TOTAL------->" + count + " Depth---->" + globalDepth);

        final int d = data.getInt(0L);
        final int s = data.getInt(4L);

        System.out.println("d=" + d + " s" + s);

    }

    public int keyCount = 0;

    public void readSequentially() throws IOException {
        int x = 0;
        long size = data.size();
        final long depth = index.getInt(0);
        final HashSet<Long> longs = new HashSet<Long>();
        for (int i = 0; i < Math.pow(2, depth); i++) {
            long address = index.getLong(bucketAddressOffsetInIndexFile(i));
            if(!longs.add(address))continue;
            final int bucketDepth = data.getInt(address);
            final int bucketSize = data.getInt(address += 4L);
            address += 4L;
            x += bucketSize;
            for (int j = 0; j < bucketSize; j++) {
                final int keyLen = data.getInt(address);
                keyCount++;
                byte[] arr = new byte[keyLen];
                data.getBytes(address += 4L, arr);
                final Data keyRead = new Data(0, arr);
                final int recordLen = data.getInt(address += keyLen);
                arr = new byte[recordLen];
                data.getBytes(address += 4L, arr);
                final Data valueRead = new Data(0, arr);
                address += recordLen;
            }
        }
        System.out.println("bs=" + x);
    }

    public Data getData(Data key) {
        final int slot = findSlot(key, globalDepth);
        long address = index.getLong(bucketAddressOffsetInIndexFile(slot));
        final int bucketSize = data.getInt(address += 4L);
        address += 4L;
        for (int j = 0; j < bucketSize; j++) {
            final int keyLen = data.getInt(address);
            keyCount++;
            byte[] arr = new byte[keyLen];
            data.getBytes(address += 4L, arr);
            final Data keyRead = new Data(0, arr);
            final int recordLen = data.getInt(address += keyLen);
            if(key.equals(keyRead)){
                arr = new byte[recordLen];
                data.getBytes(address += 4L, arr);
                final Data valueRead = new Data(0, arr);
                address += recordLen;
                return new Data(0,arr);
            }
            else {
                address += 4L;
                address += recordLen;

            }

        }

        return null;
    }

    private long bucketAddressOffsetInIndexFile(int slot) {
        return (slot * 8L) + 8L;
    }

    public int getCount() {
        return count;
    }

    public void close() {
        try {
            index.close();
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    //todo what if depth > 0?
    private int findSlot(Data key, int depth) {
        if (depth == 0) {
            return 0;
        }
        final int hash = HASHER.hash(key);
        return ((hash & (0xFFFFFFFF >>> (32 - depth))));

    }

}
