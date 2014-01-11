//package com.hazelcast.disk.core;
//
//import com.hazelcast.nio.serialization.Data;
//import com.hazelcast.util.ConcurrencyUtil;
//import com.hazelcast.util.ConstructorFunction;
//
//import java.io.FileNotFoundException;
//import java.io.IOException;
//import java.io.RandomAccessFile;
//import java.nio.ByteOrder;
//import java.nio.MappedByteBuffer;
//import java.nio.channels.FileChannel;
//import java.util.ArrayList;
//import java.util.LinkedList;
//import java.util.List;
//import java.util.Queue;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.ConcurrentMap;
//
///**
// * @author: ahmetmircik
// * Date: 12/26/13
// */
//public class IndexDir {
//    private int depth;
//    private final String path;
//    private long totalCount = 0;
//
//    private RandomAccessFile indexFile;
//    private FileChannel indexFileChannel;
//
//    private RandomAccessFile dataFile;
//    private FileChannel dataFileChannel;
//
//    public IndexDir(String path) {
//        this.path = path;
//        createFiles();
//        init();
//    }
//
//    private void createFiles() {
//        try {
//            // index file.
//            this.indexFile = new RandomAccessFile(path + ".index", "rw");
//            this.indexFileChannel = indexFile.getChannel();
//            // data file.
//            this.dataFile = new RandomAccessFile(path + ".data", "rw");
//            this.dataFileChannel = dataFile.getChannel();
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        }
//    }
//
//    private void init() {
//        final MappedByteBuffer indexFile = getIndexFile(0, 16);
//        depth = indexFile.getInt();
//        if (depth == 0) {
//            final MappedByteBuffer newBucket = getOrCreateNewBucket(0);
//            newBucket.putInt(0);
//            newBucket.putInt(0);
//
//            indexFile.position(0);
//            indexFile.putInt(0);  //depth
//            indexFile.putInt(0);  //count
//            indexFile.putInt(0);  //hash
//            indexFile.putInt(0); //address
//        }
//    }
//
//
//    public void preallocate(int depth) {
//        this.depth = depth;
//        int slots = (int) Math.pow(2, depth);
//        final int size =  slots * 8 + 8;
//        MappedByteBuffer buffer = getIndexFile(0, size);
//        buffer.putInt(depth);//depth
//        buffer.putInt(0);//number of records.
//        System.out.println(buffer.capacity());
//        int last = 0;
//          try{
//
//        for (int i = 0; i < slots; i++) {
//            last = i;
//            buffer.putInt(i);
//            buffer.putInt(getAddress(i));
//
//
////            MappedByteBuffer newBucket = getOrCreateNewBucket(i);
////            newBucket.putInt(depth);
//        }
//
//          }   catch (Exception e){
//              System.out.println("last--" + last);
//              throw new RuntimeException(e);
//          }
//    }
//
//
//    ConcurrentMap map = new ConcurrentHashMap<Integer, MappedByteBuffer>();
//    ConcurrentMap map2 = new ConcurrentHashMap<Integer, MappedByteBuffer>();
//
//    ConstructorFunction<Integer, MappedByteBuffer> function = new ConstructorFunction<Integer, MappedByteBuffer>() {
//
//        @Override
//        public MappedByteBuffer createNew(Integer depth) {
//
//            int acquireSize = (int) (Math.pow(2, depth) * 8);
//            return getIndexFile(0, acquireSize + 8);
//        }
//    };
//
//    ConstructorFunction<Integer, MappedByteBuffer> function2 = new ConstructorFunction<Integer, MappedByteBuffer>() {
//
//        @Override
//        public MappedByteBuffer createNew(Integer address) {
//            return getBucketByAddress(address);
//        }
//    };
//
//
//
//    public void insert(Data key, Data record) {
//        final int index = findSlot(key, depth);
//
////        int acquireSize = (int) (Math.pow(2, depth) * 8);
////        final MappedByteBuffer indexFile = getIndexFile(0, acquireSize + 8);
//        final MappedByteBuffer indexFile = ConcurrencyUtil.getOrPutIfAbsent(map,depth,function);
//        indexFile.position((index * 8) + 8 + 4);
//        int bucketAddress = indexFile.getInt();
////        final MappedByteBuffer bucket = getBucketByAddress(bucketAddress);
//        final MappedByteBuffer bucket = ConcurrencyUtil.getOrPutIfAbsent(map2, bucketAddress, function2);
//        bucket.position(0);
////        if (checkExists(key, bucket)) {
////            System.out.println("exist.....");
////            return;
////        }
//
//        int depthOfBucket = bucket.getInt();
//        int numberOfElementsInBucket = bucket.getInt();
//        if (numberOfElementsInBucket == Bucket.NUMBER_OF_RECORDS) {
//            if (depthOfBucket < depth) {
//                final List<Integer> addressList = newRange(index, depthOfBucket);
//                ++depthOfBucket;
//                //write new bucket depth.
//                bucket.position(0);
//                bucket.putInt(depthOfBucket);
//                // create new bucket.
//                int createIndexAt = addressList.get(0);
//                MappedByteBuffer newBucket = getOrCreateNewBucket(createIndexAt);
//                newBucket.putInt(depthOfBucket);
//                newBucket.putInt(0);//num. of elems.
//
//                indexFile.position((index * 8) + 8 + 4);
//                int oldAddress = indexFile.getInt();
//
//                for (Integer asIndex : addressList) {
//                    indexFile.position((asIndex * 8) + 8 + 4);
//                    indexFile.putInt(getAddress(createIndexAt));
//                }
//                redistributeKeysByAddress(oldAddress);
//
//                //rebalance count.
//                indexFile.position(0);
//                indexFile.putInt(depth);
//                int totCount = indexFile.getInt();
//                totCount -= Bucket.NUMBER_OF_RECORDS;
//                indexFile.position(4);
//                indexFile.putInt(totCount);
//
//
//            } else {
//                split(index);
//                redistributeKeys(index);
//            }
//            insert(key, record);
//        } else {
//            final int blockSize = 4 + 4;
//            if (blockSize > Bucket.SIZE_OF_RECORD) {
//                throw new IllegalArgumentException(blockSize + " is bigger than allowed record size "
//                        + Bucket.SIZE_OF_RECORD);
//            }
//            for (int i = 0; i < numberOfElementsInBucket; i++) {
//                final int keyLen = bucket.getInt();
//                int position = bucket.position();
//                bucket.position(position + keyLen);
//                final int recordLen = bucket.getInt();
//                position = bucket.position();
//                bucket.position(position + recordLen);
//            }
////            System.out.println(key.hashCode() + " " + key.getBuffer().length);
//
//            bucket.putInt(key.getBuffer().length);
//            bucket.put(key.getBuffer());
//            bucket.putInt(record.getBuffer().length);//rec len
//            bucket.put(record.getBuffer());//rec
//
//
////            bucket.position(bucket.position() - (8+key.getBuffer().length+record.getBuffer().length));
////            int key_len = bucket.getInt();
////            byte[] tmp = new byte[key_len];
////            bucket.get(tmp);
////            Data data = new Data(0,tmp);
////            System.out.println("R=>" + data.hashCode() + " " + data.getBuffer().length);
//
//
//            bucket.position(4);//skip depth.
//            bucket.putInt(++numberOfElementsInBucket);
//
//
//            //write total count.
//            indexFile.position(4);
//            int totalCount = indexFile.getInt();
//            indexFile.position(4);
//            indexFile.putInt(++totalCount);
////            System.out.println("total count " + totalCount);
//        }
//    }
//
//    private List<Integer> newRange(int index, int bucketDepth) {
//        final int diff = depth - bucketDepth;
//
//        String s = toBinary(index, depth);
//        String substring = reverseFirstBits(s.substring(diff - 1));
//        Queue<String> strings = new LinkedList<String>();
//        strings.add(substring);
//        String tmp;
//        while ((tmp = strings.peek()) != null && tmp.length() < depth) {
//            tmp = strings.poll();
//            strings.add("0" + tmp);
//            strings.add("1" + tmp);
//        }
//
//        List<Integer> addressList = new ArrayList<Integer>(strings.size());
//        while ((tmp = strings.poll()) != null) {
//            addressList.add(Integer.valueOf(tmp, 2));
//        }
//
//        return addressList;
//    }
//
//    String reverseFirstBits(String s) {
//        int length = s.length();
//        int i = -1;
//        String gen = "";
//        while (++i < length) {
//            int t = Character.digit(s.charAt(i), 2);
//            if (i == 0) {
//                gen += (t == 0 ? "1" : t);
//            } else {
//                gen += "" + t;
//            }
//        }
//        return gen;
//    }
//
//
//    private void split(int index) {
//        depth++;
//        //todo do not load full index??
//        int acquireSize = (int) (Math.pow(2, depth) * 8);
//        final MappedByteBuffer indexFile = getIndexFile(0, acquireSize + 8);
//        indexFile.position(8);
//        //double index table by copy.
//        final int numberOfHashSegments = (int) Math.pow(2, depth);
//        for (int i = 0; i < numberOfHashSegments / 2; i++) {
//            indexFile.position((i * 8) + 8);
//            final int hash = indexFile.getInt();
//            final int address = indexFile.getInt();
////            System.out.println(hash + "---" + address);
//            //eski bucket depth =depth
//            final int siblingIndex = depth == 1 ? 1 : Integer.valueOf("1" + toBinary(hash, depth - 1), 2);
////            final int siblingIndex = hash + (int)Math.pow(2,depth-1);
//            indexFile.position((siblingIndex * 8) + 8);
//            indexFile.putInt(siblingIndex);
//            int addressSibling;
//            if (hash == index) {
//                final MappedByteBuffer oldBucket = getOrCreateNewBucket(index);
//                oldBucket.putInt(depth);
//                final MappedByteBuffer newBucket = getOrCreateNewBucket(siblingIndex);
//                newBucket.putInt(depth);
//                newBucket.putInt(0);
//                addressSibling = getAddress(siblingIndex);
//                indexFile.putInt(addressSibling);
////                System.out.println(siblingIndex + "---" + address1);
//            } else {
//                addressSibling = address;
//                indexFile.putInt(addressSibling);
//            }
//
////            System.out.println(hash + "::" + address);
////            System.out.println(siblingIndex + "::" + addressSibling);
//        }
//
//        indexFile.position(0);
//        indexFile.putInt(depth);
//        int totCount = indexFile.getInt();
//        totCount -= Bucket.NUMBER_OF_RECORDS;
//        indexFile.position(4);
//        indexFile.putInt(totCount);
////        System.out.println("--------");
//        ///log("write depth", depth);
//    }
//
//
//    private void redistributeKeys(int index) {
//        final MappedByteBuffer bucket = getOrCreateNewBucket(index);
//        final Data[][] keyValuePairs = getKeyValuePairs(bucket);
//        bucket.position(4);
//        bucket.putInt(0); //set num. to zero.
//        for (int i = 0; i < keyValuePairs.length; i++) {
//            insert(keyValuePairs[i][0], keyValuePairs[i][1]);
//        }
//    }
//
//    private boolean checkExists(Data key, MappedByteBuffer bucket) {
//        final int bucketDepth = bucket.getInt();
//        final int numberOfElements = bucket.getInt();
//        for (int i = 0; i < numberOfElements; i++) {
//            final int keyLen = bucket.getInt();
//            byte[] bytes = new byte[keyLen];
//            bucket.get(bytes);
//            Data keyGot = new Data(0, bytes);
//            if (key.equals(keyGot)) {
//                bucket.position(0);
//                return true;
//            }
//        }
//        bucket.position(0);
//        return false;
//
//    }
//
//    private void redistributeKeysByAddress(int address) {
////        final MappedByteBuffer bucket = getBucketByAddress(address);
//        final MappedByteBuffer bucket = ConcurrencyUtil.getOrPutIfAbsent(map2, address, function2);
//        bucket.position(0);
//        final Data[][] keyValuePairs = getKeyValuePairs(bucket);
//        bucket.position(4);
//        bucket.putInt(0);
//        for (int i = 0; i < keyValuePairs.length; i++) {
//            insert(keyValuePairs[i][0], keyValuePairs[i][1]);
//        }
//    }
//
//    private Data[][] getKeyValuePairs(MappedByteBuffer bucket) {
//        bucket.position(4);
//        final int numberOfElements = bucket.getInt();
//        bucket.position(8);
//        final Data[][] data = new Data[numberOfElements][2];
//        for (int i = 0; i < numberOfElements; i++) {
//            //get key
//            final int keyLen = bucket.getInt();
////            log("keyLen ", keyLen);
//            byte[] bytes = new byte[keyLen];
//            bucket.get(bytes);
//            data[i][0] = new Data(0, bytes);
//            //get value
//            final int recordLen = bucket.getInt();
//            bytes = new byte[recordLen];
//            bucket.get(bytes);
//            data[i][1] = new Data(0, bytes);
//        }
//        return data;
//    }
//
//
////    private int[][] getKeyValuePairsTest(MappedByteBuffer bucket) {
////        bucket.position(4);
////        final int numberOfElements = bucket.getInt();
////        bucket.position(8);
////        final int[][] data = new int[numberOfElements][2];
////        for (int i = 0; i < numberOfElements; i++) {
////            //get key
////            bucket.getInt();
////            final int key = bucket.getInt();
////            bucket.getInt();
////            final int record = bucket.getInt();
////            data[i][0] = key;
////            data[i][1] = record;
////        }
////        return data;
////    }
//
//    /**
//     * generate binary prepending by zero.
//     */
//    String toBinary(int hash, int depth) {
//        String s = Integer.toBinaryString(hash);
//        while (s.length() < depth) {
//            s = "0" + s;
//        }
//        //log(s);
//        return s;
//    }
//
//    private int getAddress(int index) {
//        return index * BUCKET_LENGTH;
//    }
//
//
//    private void log(String name, Object o) {
//        System.out.println(name + "::" + o);
//    }
//
//    private static final int BUCKET_LENGTH = Bucket.BUCKET_LENGTH;
//
//    private MappedByteBuffer getOrCreateNewBucket(int index) {
//
//        final MappedByteBuffer bucket = ConcurrencyUtil.getOrPutIfAbsent(map2, BUCKET_LENGTH * index, function2);
//        bucket.position(0);
//
//        return  bucket;
//
////        //depth of bucket
////
////        MappedByteBuffer bucket = null;
////        try {
////            bucket = dataFileChannel.map(FileChannel.MapMode.READ_WRITE,
////                    BUCKET_LENGTH * index,
////                    BUCKET_LENGTH);
////            bucket.order(ByteOrder.nativeOrder());
////        } catch (Exception e) {
////            log("negative position ", index + " BUCKET_LENGTH " + BUCKET_LENGTH + " depth " + depth);
////            e.printStackTrace();
////        }
////
////        return bucket;
//    }
//
//    private MappedByteBuffer getBucketByAddress(int address) {
//
//        //depth of bucket
//
//        MappedByteBuffer bucket = null;
//        try {
//            bucket = dataFileChannel.map(FileChannel.MapMode.READ_WRITE,
//                    address,
//                    BUCKET_LENGTH);
//            bucket.order(ByteOrder.nativeOrder());
//        } catch (Exception e) {
//            log("negative position ", address + " " + BUCKET_LENGTH + "depth " + depth);
//            e.printStackTrace();
//        }
//
//        return bucket;
//    }
//
//
//    private MappedByteBuffer loadIndexFile() {
//        long length = 0;
//        try {
//            length = indexFile.length();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return getIndexFile(0, length);
//    }
//
//
//    private MappedByteBuffer getIndexFile(int offset, long size) {
//        MappedByteBuffer bucket = null;
//        try {
//            bucket = indexFileChannel.map(FileChannel.MapMode.READ_WRITE,
//                    offset, size);
//            bucket.order(ByteOrder.nativeOrder());
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        return bucket;
//    }
//
//
//    public void close() throws IOException {
//
//        System.out.println("depth--->" + depth);
//        dataFileChannel.close();
//        dataFile.close();
//        indexFileChannel.close();
//        indexFile.close();
//    }
//
//
//    // todo murmurhash3??ba
//    private static final Hasher<Data, Long> HASHER = new Hasher<Data, Long>() {
//        @Override
//        public Long hash(Data s) {
//            return s.hashCode() % 19997;
//        }
//    };
//
//    private static final Hasher<Data, Long> hasher = Hasher.DATA_HASHER;
//
//    private int findSlot(Data key, int depth) {
//        if (depth == 0) {
//            return 0;
//        }
//        long hash = hasher.hash(key);
//
//        String s = Long.toBinaryString(hash);
//        int length;
//        while ((length = s.length()) < depth) {
//            s = "0" + s;
//        }
//        int i;
//        try {
//            i = Integer.parseInt(s.substring(length - depth < 0 ? length : length - depth), 2);
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//
//        }
//        return i;
//        //return i >= Integer.MAX_VALUE ? Integer.MAX_VALUE : Math.abs(i);
//
//    }
//
//}
