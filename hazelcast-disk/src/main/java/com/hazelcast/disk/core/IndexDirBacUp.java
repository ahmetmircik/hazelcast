package com.hazelcast.disk.core;

import com.hazelcast.nio.serialization.Data;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * @author: ahmetmircik
 * Date: 12/26/13
 */
public class IndexDirBacUp {
    private int depth;
    private final String path;
    private long totalCount = 0;

    private RandomAccessFile indexFile;
    private FileChannel indexFileChannel;

    private RandomAccessFile dataFile;
    private FileChannel dataFileChannel;

    public IndexDirBacUp(String path) {
        this.path = path;
        createFiles();
        init();
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

    private void init() {
        final MappedByteBuffer indexFile = getIndexFile(0, 16);
        depth = indexFile.getInt();
        if (depth == 0) {
            final MappedByteBuffer newBucket = getOrCreateNewBucket(0);
            newBucket.putInt(0);
            newBucket.putInt(0);

            indexFile.position(0);
            indexFile.putInt(0);  //depth
            indexFile.putInt(0);  //count
            indexFile.putInt(0);  //hash
            indexFile.putInt(0); //address
        }
    }

    int currentKey;
    public void insert(int key, int record) {
        currentKey = key;
        final int index = makeAddress(key, depth);
        //System.out.println("*** " + key.hashCode() + "-" + index);

        int acquireSize = (int) (Math.pow(2, depth) * 8);
        final MappedByteBuffer indexFile = getIndexFile(0, acquireSize + 8);
        indexFile.position((index * 8) + 8 + 4);
        int bucketAddress = indexFile.getInt();
        final MappedByteBuffer bucket = getBucketByAddress(bucketAddress);
//        if( checkExists(key,bucket) ){
//            System.out.println("exist.....");
//            return;
//        }

        int depthOfBucket = bucket.getInt();
        int numberOfElementsInBucket = bucket.getInt();
        if (numberOfElementsInBucket == Bucket.NUMBER_OF_RECORDS) {
            if (depthOfBucket < depth) {
                final List<Integer> addressList = newRange(index, depthOfBucket);
                ++depthOfBucket;
                //write new bucket depth.
                bucket.position(0);
                bucket.putInt(depthOfBucket);
                // create new bucket.
                int createIndexAt = addressList.get(0);
                MappedByteBuffer newBucket = getOrCreateNewBucket(createIndexAt);
                newBucket.putInt(depthOfBucket);
                newBucket.putInt(0);//num. of elems.

                indexFile.position((index * 8) + 8 + 4);
                int oldAddress = indexFile.getInt();

                for (Integer asIndex : addressList) {
                    indexFile.position((asIndex * 8) + 8 + 4);
                    indexFile.putInt(getAddress(createIndexAt));
                }
                redistributeKeysByAddress(oldAddress);

                //rebalance count.
                indexFile.position(0);
                indexFile.putInt(depth);
                int totCount = indexFile.getInt();
                totCount -= Bucket.NUMBER_OF_RECORDS;
                indexFile.position(4);
                indexFile.putInt(totCount);


            } else {
                split(index);
                redistributeKeys(index);
            }
            insert(key, record);
        } else {
            final int blockSize = 4 + 4;
            if (blockSize > Bucket.SIZE_OF_RECORD) {
                throw new IllegalArgumentException(blockSize + " is bigger than allowed record size "
                        + Bucket.SIZE_OF_RECORD);
            }
            for (int i = 0; i < numberOfElementsInBucket; i++) {
                final int keyLen = bucket.getInt();
                int position = bucket.position();
                bucket.position(position + keyLen);
                final int recordLen = bucket.getInt();
                position = bucket.position();
                bucket.position(position + recordLen);
            }
            bucket.putInt(4);
            bucket.putInt(key);
            bucket.putInt(4);//rec len
            bucket.putInt(record);//rec

            bucket.position(4);//skip depth.
            bucket.putInt(++numberOfElementsInBucket);

            //write total count.
            indexFile.position(4);
            int totalCount = indexFile.getInt();
            indexFile.position(4);
            indexFile.putInt(++totalCount);
//            System.out.println("total count " + totalCount);
        }
    }

    private List<Integer> newRange(int index, int bucketDepth) {
        final int diff = depth - bucketDepth;

        String s = toBinary(index, depth);
        String substring = reverseFirstBits(s.substring(diff-1));
        Queue<String> strings = new LinkedList<String>();
        strings.add(substring);
        String tmp;
        while ((tmp = strings.peek()) != null && tmp.length() < depth) {
            tmp = strings.poll();
            strings.add("0" + tmp);
            strings.add("1" + tmp);
        }

        List<Integer> addressList = new ArrayList<Integer>(strings.size());
        while ((tmp = strings.poll()) != null) {
            addressList.add(Integer.valueOf(tmp, 2));
        }

        return addressList;
    }

    String reverseFirstBits(String s) {
        int length = s.length();
        int i = -1;
        String gen = "";
        while (++i < length) {
            int t = Character.digit(s.charAt(i),2);
            if(i==0){
                gen += (t == 0 ? "1" : t) ;
            }
            else {
                gen += ""+t;
            }
        }
        return gen;
    }



    private void split(int index) {
//        System.out.println("--------");
        // inc depth.
        depth++;
        //todo do not load full index??
        int acquireSize = (int) (Math.pow(2, depth) * 8);
        final MappedByteBuffer indexFile = getIndexFile(0, acquireSize + 8);
        indexFile.position(8);
        //double index table by copy.
        final int numberOfHashSegments = (int) Math.pow(2, depth);
        for (int i = 0; i < numberOfHashSegments / 2; i++) {
            indexFile.position((i * 8) + 8);
            final int hash = indexFile.getInt();
            final int address = indexFile.getInt();
//            System.out.println(hash + "---" + address);
            //eski bucket depth =depth
            final int siblingIndex = depth == 1 ? 1 : Integer.valueOf("1" + toBinary(hash, depth - 1), 2);
//            final int siblingIndex = hash + (int)Math.pow(2,depth-1);
//            System.out.println(index +"______"+siblingIndex);
            indexFile.position((siblingIndex * 8) + 8);
            indexFile.putInt(siblingIndex);
            int addressSibling;
            if (hash == index) {
                final MappedByteBuffer oldBucket = getOrCreateNewBucket(index);
                oldBucket.putInt(depth);
                final MappedByteBuffer newBucket = getOrCreateNewBucket(siblingIndex);
                newBucket.putInt(depth);
                newBucket.putInt(0);
                addressSibling = getAddress(siblingIndex);
                indexFile.putInt(addressSibling);
//                System.out.println(siblingIndex + "---" + address1);
            } else {
                addressSibling = address;
                indexFile.putInt(addressSibling);
            }

//            System.out.println(hash + "::" + address);
//            System.out.println(siblingIndex + "::" + addressSibling);
        }

        indexFile.position(0);
        indexFile.putInt(depth);
        int totCount = indexFile.getInt();
        totCount -= Bucket.NUMBER_OF_RECORDS;
        indexFile.position(4);
        indexFile.putInt(totCount);
//        System.out.println("--------");
        ///log("write depth", depth);
    }


    private void redistributeKeys(int index) {
        final MappedByteBuffer bucket = getOrCreateNewBucket(index);
        final int[][] keyValuePairs = getKeyValuePairsTest(bucket);
        bucket.position(4);
        bucket.putInt(0); //set num. to zero.
        for (int i = 0; i < keyValuePairs.length; i++) {
            insert(keyValuePairs[i][0], keyValuePairs[i][1]);
        }
    }

    private void redistributeKeysByAddress(int address) {
        final MappedByteBuffer bucket = getBucketByAddress(address);
        final int[][] keyValuePairs = getKeyValuePairsTest(bucket);
        bucket.position(4);
        bucket.putInt(0);
        for (int i = 0; i < keyValuePairs.length; i++) {
            insert(keyValuePairs[i][0], keyValuePairs[i][1]);
        }
    }

    private Data[][] getKeyValuePairs(MappedByteBuffer bucket) {
        bucket.position(4);
        final int numberOfElements = bucket.getInt();
        bucket.position(8);
        final Data[][] data = new Data[numberOfElements][2];
        for (int i = 0; i < numberOfElements; i++) {
            //get key
            final int keyLen = bucket.getInt();
//            log("keyLen ", keyLen);
            byte[] bytes = new byte[keyLen];
            bucket.get(bytes);
            data[i][0] = new Data(0, bytes);
            //get value
            final int recordLen = bucket.getInt();
            bytes = new byte[recordLen];
            bucket.get(bytes);
            data[i][1] = new Data(0, bytes);
        }
        return data;
    }


    private int[][] getKeyValuePairsTest(MappedByteBuffer bucket) {
        bucket.position(4);
        final int numberOfElements = bucket.getInt();
        bucket.position(8);
        final int[][] data = new int[numberOfElements][2];
        for (int i = 0; i < numberOfElements; i++) {
            //get key
            bucket.getInt();
            final int key = bucket.getInt();
            bucket.getInt();
            final int record = bucket.getInt();
            data[i][0] = key;
            data[i][1] = record;
        }
        return data;
    }

    /**
     * generate binary prepending by zero.
     */
    String toBinary(int hash, int depth) {
        String s = Integer.toBinaryString(hash);
        while (s.length() < depth) {
            s = "0" + s;
        }
        //log(s);
        return s;
    }

    private int getAddress(int index) {
        return index * BUCKET_LENGTH;
    }


    private void log(String name, Object o) {
        System.out.println(name + "::" + o);
    }

    private static final int BUCKET_LENGTH = 4 +
            // bucket size info. bits
            4 +
            // key + value sizes
            (Bucket.NUMBER_OF_RECORDS * Bucket.SIZE_OF_RECORD);

    private MappedByteBuffer getOrCreateNewBucket(int index) {

        //depth of bucket

        MappedByteBuffer bucket = null;
        try {
            bucket = dataFileChannel.map(FileChannel.MapMode.READ_WRITE,
                    BUCKET_LENGTH * index,
                    BUCKET_LENGTH);
            bucket.order(ByteOrder.nativeOrder());
        } catch (Exception e) {
            System.out.println("Key-->"+currentKey +" Negative position index -->"+index + " Bucket length-->" + BUCKET_LENGTH + " Depth-->" + depth);
            e.printStackTrace();
        }

        return bucket;
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
            log("negative position ", address + " " + BUCKET_LENGTH + "depth " + depth);
            e.printStackTrace();
        }

        return bucket;
    }


    private MappedByteBuffer loadIndexFile() {
        long length = 0;
        try {
            length = indexFile.length();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return getIndexFile(0, length);
    }


    private MappedByteBuffer getIndexFile(int offset, long size) {
        MappedByteBuffer bucket = null;
        try {
            bucket = indexFileChannel.map(FileChannel.MapMode.READ_WRITE,
                    offset, size);
            bucket.order(ByteOrder.nativeOrder());
        } catch (Exception e) {
            System.out.println("size is -->" + size+ " depth-->"+depth);
            e.printStackTrace();
        }

        return bucket;
    }


    public void close() throws IOException {
        dataFileChannel.close();
        dataFile.close();
        indexFileChannel.close();
        indexFile.close();
    }


    // todo murmurhash3??ba
    private static final Hasher<Data, Integer> HASHER = new Hasher<Data, Integer>() {
        @Override
        public Integer hash(Data s) {
            return s.hashCode() % 19997;
        }
    };

    private int makeAddress(Integer key, int depth) {
        if (depth == 0) {
            return 0;
        }

        byte[] bytes = (key + "").getBytes();
        final long hash = MurmurHash3.murmurhash3x8632(bytes, 0, bytes.length, 129);
        //Arrays.hashCode(key.getBuffer());
        String s = Long.toBinaryString(hash);
        //log("s",s);
        int length = 0;
        while ((length = s.length()) < depth) {
            s = "0" + s;
        }
        int i = 0;
        try {
            i = Integer.parseInt(s.substring(length - depth < 0 ? length : length - depth), 2);
        } catch (Exception e) {
            e.printStackTrace();

        }
        return i >= Integer.MAX_VALUE ? Integer.MAX_VALUE : Math.abs(i);
    }


    private boolean checkExists(Integer key,MappedByteBuffer bucket) {
        final int bucketDepth = bucket.getInt();
        final int numberOfElements = bucket.getInt();
        for (int i = 0; i < numberOfElements; i++) {
            final int keyLen = bucket.getInt();
            int keyGot = bucket.getInt();
            if(key.equals(keyGot)){
                bucket.position(0);
                return true;
            }
        }
        bucket.position(0);
        return false;

    }


}
