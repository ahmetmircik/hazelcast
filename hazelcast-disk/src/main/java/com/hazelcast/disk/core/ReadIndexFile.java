package com.hazelcast.disk.core;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author: ahmetmircik
 * Date: 12/27/13
 */
public class ReadIndexFile {

    private String path;

    private RandomAccessFile indexFile;
    private FileChannel indexFileChannel;

    private RandomAccessFile dataFile;
    private FileChannel dataFileChannel;

    private int total;
    int globalDepth;

    int globalCount;
    long fileLen;


    public ReadIndexFile(String path) {
        this.path = path;
        createFiles();
        init();
    }

    void init() {
        fileLen = 0;
        try {
            fileLen = indexFile.length();
        } catch (IOException e) {
            e.printStackTrace();
        }
        final MappedByteBuffer indexFile = getIndexFile(0, fileLen);
        globalDepth = indexFile.getInt();
        globalCount = indexFile.getInt();
        System.out.println("TOTAL------->" + globalCount);
    }

    private void createFiles() {
        try {
            // index file.
            this.indexFile = new RandomAccessFile(path + ".index", "rw");
            this.indexFileChannel = indexFile.getChannel();
            // data file.
            this.dataFile = new RandomAccessFile(path + ".data", "rw");
            this.dataFileChannel = dataFile.getChannel();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private MappedByteBuffer getIndexFile(int offset, long size) {
        MappedByteBuffer bucket = null;
        try {
            bucket = indexFileChannel.map(FileChannel.MapMode.READ_WRITE,
                    offset, size);
            bucket.order(ByteOrder.nativeOrder());
        } catch (IOException e) {
            e.printStackTrace();
        }

        return bucket;
    }

    private MappedByteBuffer getDataFile(int offset, long size) {
        MappedByteBuffer bucket = null;
        try {
            bucket = dataFileChannel.map(FileChannel.MapMode.READ_WRITE,
                    offset, size);
            bucket.order(ByteOrder.nativeOrder());
        } catch (IOException e) {
            e.printStackTrace();
        }

        return bucket;
    }

    //depth of bucket
    static final int BUCKET_LENGTH = 4 +
            4 +
            Bucket.NUMBER_OF_RECORDS * Bucket.SIZE_OF_RECORD;
    Set<Integer> s = new HashSet<Integer>();

    public void print() throws IOException {

        System.out.println("-----------" + path + "-----------------------");
        System.out.println("depth " + globalDepth);
        System.out.println("count " + globalCount);
        final MappedByteBuffer indexFile = getIndexFile(0, fileLen);
        int length = (int) fileLen;
        length -= 8;
        System.out.println("hash\t---\taddress");
        while (length > 0) {
            final int hash = indexFile.getInt();
            final int address = indexFile.getInt();
            if (s.add(address)) {
                final MappedByteBuffer dataFile = getDataFile(address, BUCKET_LENGTH);

                getRecord(dataFile);
            }

            System.out.println(hash + "\t---\t" + address);
            length -= 8;
        }
        System.out.println("-----------" + path + "-----------------------");
        System.out.println("total = " + total);

        System.out.println("x = " + s.size());

    }

    public void getValue(Data key) {
        final int index = findSlot(key, globalDepth);
        final MappedByteBuffer indexFile = ConcurrencyUtil.getOrPutIfAbsent(map, globalDepth, function);
        indexFile.position((index * 8) + 8 + 4);
        final int bucketAddress = indexFile.getInt();
//        MappedByteBuffer bucketByAddress = getBucketByAddress(bucketAddress);
        MappedByteBuffer bucketByAddress = ConcurrencyUtil.getOrPutIfAbsent(map2, bucketAddress, function2);
        if (checkExists(key, bucketByAddress)) {
//           System.out.println("TRUE");
        } else {
//            System.out.println("FALSE");
        }
    }

    private MappedByteBuffer getBucketByAddress(int address) {

        //depth of bucket

        MappedByteBuffer bucket = null;
        try {
            bucket = dataFileChannel.map(FileChannel.MapMode.READ_WRITE,
                    address,
                    BUCKET_LENGTH);
            bucket.order(ByteOrder.nativeOrder());
        } catch (Exception e) {
            e.printStackTrace();
        }

        return bucket;
    }

    ConcurrentMap map = new ConcurrentHashMap<Integer, MappedByteBuffer>();
    ConcurrentMap map2 = new ConcurrentHashMap<Integer, MappedByteBuffer>();

    ConstructorFunction<Integer, MappedByteBuffer> function = new ConstructorFunction<Integer, MappedByteBuffer>() {

        @Override
        public MappedByteBuffer createNew(Integer depth) {

            int acquireSize = (int) (Math.pow(2, depth) * 8);
            return getIndexFile(0, acquireSize + 8);
        }
    };

    ConstructorFunction<Integer, MappedByteBuffer> function2 = new ConstructorFunction<Integer, MappedByteBuffer>() {

        @Override
        public MappedByteBuffer createNew(Integer address) {
            return getBucketByAddress(address);
        }
    };
    private static final Hasher<Data, Integer> hasher = Hasher.DATA_HASHER;

    private int findSlot(Data key, int depth) {
        if (depth == 0) {
            return 0;
        }
        long hash = hasher.hash(key);

        String s = Long.toBinaryString(hash);
        int length;
        while ((length = s.length()) < depth) {
            s = "0" + s;
        }
        int i;
        try {
            i = Integer.parseInt(s.substring(length - depth < 0 ? length : length - depth), 2);
        } catch (Exception e) {
            throw new RuntimeException(e);

        }
        return i;
        //return i >= Integer.MAX_VALUE ? Integer.MAX_VALUE : Math.abs(i);

    }

//    private int makeAddress(Data key, int depth) {
//        if (depth == 0) {
//            return 0;
//        }
//        byte[] buffer = key.getBuffer();
//        final long hash = MurmurHash3.murmurhash3x8632(buffer, 0, buffer.length, 129);
//
//        String s = Long.toBinaryString(hash);
//        //log("s",s);
//        int length = 0;
//        while ((length = s.length()) < depth) {
//            s = "0" + s;
//        }
//        int i = 0;
//        try {
//            i = Integer.parseInt(s.substring(length - depth < 0 ? length : length - depth), 2);
//        } catch (Exception e) {
//            e.printStackTrace();
//
//        }
//        return i >= Integer.MAX_VALUE ? Integer.MAX_VALUE : Math.abs(i);
//    }


    private boolean checkExists(Data key, MappedByteBuffer bucket) {
        final int bucketDepth = bucket.getInt();
        final int numberOfElements = bucket.getInt();
        for (int i = 0; i < numberOfElements; i++) {
            final int keyLen = bucket.getInt();
            byte[] bytes = new byte[keyLen];
            bucket.get(bytes);
            Data keyGot = new Data(0, bytes);
            if (key.equals(keyGot)) {
                bucket.position(0);
                return true;
            }
            int recordLen = bucket.getInt();
            bucket.position(bucket.position() + recordLen);
        }
        bucket.position(0);
        return false;

    }

    private void getRecord(MappedByteBuffer bucket) {
        final int bucketDepth = bucket.getInt();
        final int numberOfElements = bucket.getInt();
        //System.out.println(numberOfElements);
        total += numberOfElements;
        for (int i = 0; i < numberOfElements; i++) {
            final int keyLen = bucket.getInt();
            byte[] bytes = new byte[keyLen];
            bucket.get(bytes);
            Data keyGot = new Data(0, bytes);
            final int recordLen = bucket.getInt();
            bytes = new byte[recordLen];
            bucket.get(bytes);
            Data recordGot = new Data(0, bytes);
//            System.out.println("keyGot "+keyGot.getBuffer().length);
//            System.out.println("recordGot "+recordGot.getBuffer().length);
        }


    }

    public void close() {
        try {
            indexFileChannel.close();
            indexFile.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        final ReadIndexFile readIndexFile = new ReadIndexFile("293959660");
        readIndexFile.print();
    }

}
