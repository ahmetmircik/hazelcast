package com.hazelcast.disk.core;

import com.hazelcast.disk.Storage;
import com.hazelcast.nio.serialization.Data;

import java.io.IOError;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

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
        data = new MappedView(this.path + ".data", HashTable2.DATA_BLOCK_LENGTH);
        index = new MappedView(this.path + ".index", HashTable2.INDEX_BLOCK_LENGTH);
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

    public Map<Data,Data> readSequentially() throws IOException {
        final HashMap<Data, Data> dataDataHashMap = new HashMap<Data, Data>();
        int x = 0;
        long size = data.size();
        final HashSet<Long> longs = new HashSet<Long>();
        for (int i = 0; i < size/HashTable2.BUCKET_LENGTH; i++) {
            long address = i * HashTable2.BUCKET_LENGTH;
            final long log = address;
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
                final int recordLen = data.getInt(address += keyLen * 1L);
                arr = new byte[recordLen];
                data.getBytes(address += 4L, arr);
                final Data valueRead = new Data(0, arr);
                address += recordLen * 1L;

//                System.out.println(keyRead.hashCode() + "---" + log);


                dataDataHashMap.put(keyRead,valueRead);
            }
        }
        System.out.println("bs=" + x);
        return dataDataHashMap;
    }

    public Data getData(Data key) {
//        printIndexFile();
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

    void printIndexFile(){

        final int numberOfSlots = (int) Math.pow(2, globalDepth);
        int depth = index.getInt(0);
        int count = index.getInt(4);
//        System.out.println("======================");
//        System.out.println("depth = "+depth);
//        System.out.println("count = "+count);
        for (int i = 0; i < numberOfSlots; i++) {
            index.getLong(bucketAddressOffsetInIndexFile(i));
//            System.out.println("["+i+"]"+index.getLong(bucketAddressOffsetInIndexFile(i)));
        }
//        System.out.println("======================");
    }

    private long bucketAddressOffsetInIndexFile(int slot) {
        return 1L *(slot << 3) + 8L;
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
