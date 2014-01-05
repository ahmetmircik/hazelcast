package com.hazelcast.disk.core;

import com.hazelcast.disk.Storage;
import com.hazelcast.nio.serialization.Data;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

/**
 * @author: ahmetmircik
 * Date: 12/31/13
 */
public class HashTable implements Closeable {

    private static final Hasher<Data, Integer> hasher = Hasher.DATA_HASHER;
    public static final int NUMBER_OF_RECORDS = 20;
    public static final int BLOCK_SIZE = 1024;//KVP
    public static final int SIZE_OF_RECORD = 8 + BLOCK_SIZE;
    public static final int BUCKET_LENGTH = 4 +
            // bucket size info. bits
            4 +
            // key + value sizes
            (NUMBER_OF_RECORDS * SIZE_OF_RECORD);

    private Storage data;
    private Storage index;
    private final String path;
    private int globalDepth;

    public HashTable(String path) {
        this.path = path;
        createStores();
        init();
    }

    private void createStores() {
        data = new MappedView(path + ".data", BUCKET_LENGTH);
        index = new MappedView(path + ".index", 8);
    }

    private void init() {
        globalDepth = index.getInt(0);
        if (globalDepth == 0) {
            data.writeInt(0, 0);
            data.writeInt(4, 0);
            index.writeInt(0, globalDepth);  //depth
            index.writeInt(4, 0);  //count
            index.writeInt(8, 0);  //hash
            index.writeInt(12, 0); //address
        }
    }

    private int getAddressPositionFromIndexFile(int slot) {
        return (slot * 8) + 8 + 4;
    }

    public void put(Data key, Data value) {
        final int slot = findSlot(key, globalDepth);
        final int bucketAddress = index.getInt(getAddressPositionFromIndexFile(slot));
        int bucketDepth = data.getInt(bucketAddress);
        int bucketElementsCount = data.getInt(bucketAddress + 4);
        if (bucketElementsCount == NUMBER_OF_RECORDS) {
            if (bucketDepth < globalDepth) {
                final int[] addressList = newRange(slot, bucketDepth);
                ++bucketDepth;
                //write buckets new depth.
                data.writeInt(bucketAddress, bucketDepth);
                // create new bucket.
                final int createIndexAt = addressList[0];
                data.writeInt(getAddress(createIndexAt), bucketDepth);
                data.writeInt(getAddress(createIndexAt) + 4, 0); //number of records.
                //find old address
                final int oldAddress = index.getInt(getAddressPositionFromIndexFile(slot));
                //update buddies.
                for (final int asIndex : addressList) {
                    index.writeInt(getAddressPositionFromIndexFile(asIndex), getAddress(createIndexAt));
                }
                redistributeKeys(oldAddress);
                //update index file.
                index.writeInt(0, globalDepth);
                int totCount = index.getInt(4);
                totCount -= NUMBER_OF_RECORDS;
                index.writeInt(4, totCount);
            }
            else {
                split(slot);
                redistributeKeys(getAddress(slot));
            }
            //todo remove recursions.
            put(key, value);
        } else {
            final int blockSize = BLOCK_SIZE;
            if (blockSize > SIZE_OF_RECORD) {
                throw new IllegalArgumentException(blockSize + " is bigger than allowed record size "
                        + SIZE_OF_RECORD);
            }
            int tmpBucketAddress = bucketAddress;
            tmpBucketAddress += 8;
            for (int i = 0; i < bucketElementsCount; i++) {
                final int keyLen = data.getInt(tmpBucketAddress);
                if(keyLen < 0){
                    System.out.println("key000");
                }
                tmpBucketAddress += keyLen;
                tmpBucketAddress += 4;
                final int recordLen = data.getInt(tmpBucketAddress);
                if(recordLen < 0){
                    System.out.println("rec000");
                }
                tmpBucketAddress += recordLen;
                tmpBucketAddress += 4;
            }
            final int keyLength = key.getBuffer().length;
            data.writeInt(tmpBucketAddress, keyLength);
            tmpBucketAddress += 4;
            data.writeBytes(tmpBucketAddress, key.getBuffer());
            tmpBucketAddress += keyLength;
            final int valueLength = value.getBuffer().length;
            data.writeInt(tmpBucketAddress, valueLength);
            tmpBucketAddress += 4;
            data.writeBytes(tmpBucketAddress, value.getBuffer());
            data.writeInt(bucketAddress + 4, ++bucketElementsCount);
            //write total count.
            int totalCount = index.getInt(4);
            index.writeInt(4, ++totalCount);

//            System.out.println("totalCount\t" + totalCount);
//            System.out.println("globalDepth\t"+globalDepth);
        }


    }

    // 20640
    private void split(int slot) {
        globalDepth++;
        //double index file by copy.
        final int numberOfSlots = (int) Math.pow(2, globalDepth);
        for (int i = 0; i < numberOfSlots / 2; i++) {
            final int hash = index.getInt((i * 8) + 8);
            final int address = index.getInt(getAddressPositionFromIndexFile(i));
            final int siblingSlot = globalDepth == 1 ? 1 : Integer.valueOf("1" + toBinary(hash, globalDepth - 1), 2);
            index.writeInt((siblingSlot * 8) + 8, siblingSlot);
            if (hash == slot) {
                //old bucket
                data.writeInt(getAddress(slot), globalDepth);
                //new bucket
                final int address1 = getAddress(siblingSlot);
                data.writeInt(address1, globalDepth);
                data.writeInt(getAddress(siblingSlot) + 4, 0);
                //update index file.
                index.writeInt(getAddressPositionFromIndexFile(siblingSlot), getAddress(siblingSlot));
            } else {
                //if no need to create bucket, just point old bucket.
                index.writeInt(getAddressPositionFromIndexFile(siblingSlot), address);
            }
            //System.out.println("siblingSlot\t"+siblingSlot + " globalDepth\t"+globalDepth);
        }
        //update index file.
        index.writeInt(0, globalDepth);
        int totCount = index.getInt(4);
        totCount -= NUMBER_OF_RECORDS;
        index.writeInt(4, totCount);
    }

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

    private int[] newRange(int index, final int bucketDepth) {
        final int diff = globalDepth - bucketDepth;
        final String binary = toBinary(index, globalDepth);
        final String substring = reverseFirstBits(binary.substring(diff - 1));
        final Queue<String> strings = new LinkedList<String>();
        strings.add(substring);
        String tmp;
        while ((tmp = strings.peek()) != null && tmp.length() < globalDepth) {
            tmp = strings.poll();
            strings.add("0" + tmp);
            strings.add("1" + tmp);
        }
        final int[] addressList = new int[strings.size()];
        int i = 0;
        while ((tmp = strings.poll()) != null) {
            addressList[i++] = Integer.valueOf(tmp, 2);
        }
        return addressList;
    }

    private String reverseFirstBits(String s) {
        int length = s.length();
        int i = -1;
        String gen = "";
        while (++i < length) {
            int t = Character.digit(s.charAt(i), 2);
            if (i == 0) {
                gen += (t == 0 ? "1" : t);
            } else {
                gen += "" + t;
            }
        }
        return gen;
    }

    /**
     * generate binary prepending by zero.
     */
    private String toBinary(int hash, int depth) {
        String s = Integer.toBinaryString(hash);
        while (s.length() < depth) {
            s = "0" + s;
        }
        return s;
    }

    private int getAddress(int slot) {
        return slot * BUCKET_LENGTH;
    }

    private Data[][] getKeyValuePairs(final int bucketOffset) {
        final int numberOfElements = data.getInt(bucketOffset + 4);
        final Data[][] dataObjects = new Data[numberOfElements][2];
        int tmpBucketOffset = bucketOffset;
        tmpBucketOffset += 8;
        for (int i = 0; i < numberOfElements; i++) {
            final int keyLen = data.getInt(tmpBucketOffset);
            byte[] bytes = new byte[keyLen];
            tmpBucketOffset += 4;
            data.getBytes(tmpBucketOffset, bytes);
            tmpBucketOffset += keyLen;
            dataObjects[i][0] = new Data(0, bytes);
            final int recordLen = data.getInt(tmpBucketOffset);
            bytes = new byte[recordLen];
            tmpBucketOffset += 4;
            data.getBytes(tmpBucketOffset, bytes);
            tmpBucketOffset += recordLen;
            dataObjects[i][1] = new Data(0, bytes);
        }
        //reset number of elements in this bucket.
        data.writeInt(bucketOffset + 4, 0);
        return dataObjects;
    }


    private void redistributeKeys(int address) {
        final Data[][] keyValuePairs = getKeyValuePairs(address);
        for (int i = 0; i < keyValuePairs.length; i++) {
            put(keyValuePairs[i][0], keyValuePairs[i][1]);
        }
    }


    @Override
    public void close() throws IOException {
        index.close();
        data.close();
    }
}
