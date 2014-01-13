//package com.hazelcast.disk.core;
//
//import com.hazelcast.disk.Storage;
//import com.hazelcast.nio.serialization.Data;
//
//import java.io.IOError;
//import java.io.IOException;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.Map;
//
///**
// * @author: ahmetmircik
// * Date: 12/27/13
// */
//public class ReadIndexFile {
//
//    private static final Hasher<Data, Integer> HASHER = Hasher.DATA_HASHER;
//
//    private String path;
//
//    private Storage index;
//    private Storage data;
//
//    int globalDepth;
//
//    long count;
//
//    public ReadIndexFile(String path) {
//        this.path = path;
//        data = new MappedView(this.path + ".data", HashTable2.DATA_BLOCK_LENGTH);
//        index = new MappedView(this.path + ".index", HashTable2.INDEX_BLOCK_LENGTH);
//        init();
//    }
//
//    void init() {
//        globalDepth = index.getInt(0L);
//        count = index.getLong(4L);
//        System.out.println("TOTAL------->" + count + " Depth---->" + globalDepth);
//
//        final int d = data.getInt(0L);
//        final int s = data.getInt(4L);
//
//        System.out.println("d=" + d + " s" + s);
//
//
//    }
//
//    public int keyCount = 0;
//
//    public Map<Data,Data> readSequentially() throws IOException {
//        final HashMap<Data, Data> dataDataHashMap = new HashMap<Data, Data>();
//        long size = data.size();
//        final HashSet<Long> longs = new HashSet<Long>();
//        for (int i = 0; i < size/HashTable2.BUCKET_LENGTH; i++) {
//            long address = 1L * i * HashTable2.BUCKET_LENGTH;
//            final int bucketDepth = data.getInt(address);
//            address += 4L;
//            final int bucketSize = data.getInt(address);
//            address += 4L;
//            for (int j = 0; j < bucketSize;) {
//                final byte header = data.getByte(address);
//                final boolean isRemmovedRecord = header == MARK_REMOVED;
//                address += ONE_RECORD_HEADER_SIZE;//header
//                final int keyLen = data.getInt(address);
//                address += 4L;
//                final int recordLen = data.getInt(address);
//                address += 4L;
//                byte[] arr = new byte[keyLen];
//                data.getBytes(address, arr);
//                final Data keyRead = new Data(0, arr);
//                address += keyLen;
//
//                if (key.equals(keyRead)) {
//                    if (isRemmovedRecord) {
//                        // already removed record.
//                        return null;
//                    }
//                    arr = new byte[recordLen];
//                    data.getBytes(address, arr);
//                    final Data valueRead = new Data(0, arr);
//                    return valueRead;
//                }
//                else {
//                    address += recordLen;
//                }
//
//                if (!isRemmovedRecord){
//                    j++;
//                }
//
//            }
//        }
////        System.out.println("bs=" + x);
//        return dataDataHashMap;
//    }
//
//
//
//    void printIndexFile(){
//
//        final int numberOfSlots = (int) Math.pow(2, globalDepth);
//        int depth = index.getInt(0);
//        int count = index.getInt(4);
////        System.out.println("======================");
////        System.out.println("depth = "+depth);
////        System.out.println("count = "+count);
//        for (int i = 0; i < numberOfSlots; i++) {
//            index.getLong(bucketAddressOffsetInIndexFile(i));
////            System.out.println("["+i+"]"+index.getLong(bucketAddressOffsetInIndexFile(i)));
//        }
////        System.out.println("======================");
//    }
//
//    private long bucketAddressOffsetInIndexFile(int slot) {
//        return ( ((long)slot) << 3) + 8L;
//    }
//
//    public long getCount() {
//        return count;
//    }
//
//    public void close() {
//        try {
//            index.close();
//        } catch (IOException e) {
//            throw new IOError(e);
//        }
//    }
//
//    private int findSlot(Data key, int depth) {
//        if (depth > 31) throw new IllegalArgumentException("depth is not supported\t:" + depth);
//        if (depth == 0) {
//            return 0;
//        }
//        final int hash = HASHER.hash(key);
//        return ((hash & (0x7FFFFFFF >>> (32 - depth))));
//
//    }
//
//
//}
