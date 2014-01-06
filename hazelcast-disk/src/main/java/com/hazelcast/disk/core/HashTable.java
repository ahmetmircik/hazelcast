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
 * <p/>
 * <p/>
 * <p/>
 * IndexFile Format
 * -------------------------
 * Position : 0 --> depth
 * Position : 4 --> number of records
 * Position : 8 --> hash (32 bit) & address (64 bit)
 * Position : 20 -->hash (32 bit) & address (64 bit)
 * Position : 32 -->hash (32 bit) & address (64 bit)
 * ...
 * ...
 * ...
 * <p/>
 * DataFile Format
 * -------------------------
 * Position : 0 --> 1st Bucket : bucket depth (32 bit) & number of records (32 bit) & KVP & KVP &...
 * Position : BUCKET_LENGTH --> 2nd Bucket : bucket depth (32 bit) & number of records (32 bit) & KVP & KVP &...
 * Position : 2 * BUCKET_LENGTH --> 3rd Bucket : bucket depth (32 bit) & number of records (32 bit) & KVP & KVP &...
 * Position : 3 * BUCKET_LENGTH --> 3rd Bucket : bucket depth (32 bit) & number of records (32 bit) & KVP & KVP &...
 * ...
 * ...
 * ...
 */
public class HashTable implements Closeable {

    private static final Hasher<Data, Integer> HASHER = Hasher.DATA_HASHER;
    private static final int NUMBER_OF_RECORDS = 20;
    private static final int KVP_TOTAL_SIZE = 1024*10+16;//KVP
    private static final int SIZE_OF_RECORD = 8 + KVP_TOTAL_SIZE;
    private static final int BUCKET_LENGTH = 4 + 4 + (NUMBER_OF_RECORDS * SIZE_OF_RECORD);
    private static final int INDEX_BLOCK_LENGTH =  1 << 3;
    private static final int DATA_BLOCK_LENGTH =  1 << 30;


    private final String path;
    private final Storage data;
    private final Storage index;
    private int globalDepth;

    public HashTable(String path) {
        this.path = path;
        data = new MappedView(this.path + ".data", DATA_BLOCK_LENGTH);
        index = new MappedView(this.path + ".index", INDEX_BLOCK_LENGTH);
        init();
        System.out.println(BUCKET_LENGTH);
    }

    private void init() {
        globalDepth = index.getInt(0);
        if (globalDepth == 0) {
            data.writeInt(0, 0);
            data.writeInt(4, 0);
            index.writeInt(0, globalDepth);  //depth
            index.writeInt(4, 0);  //count
            index.writeInt(8, 0);  //hash
            index.writeLong(12, 0L); //address
        }
    }

    public void put(Data key, Data value) {
        final int slot = findSlot(key, globalDepth);
        final long bucketAddress = index.getLong(bucketAddressOffsetInIndexFile(slot));
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
                final long newBucketAddress = newAddress(createIndexAt);
                data.writeInt(newBucketAddress, bucketDepth);
                data.writeInt(newBucketAddress + 4, 0); //number of records.
                //find old address
                final long oldAddress = bucketAddress;
                //update buddies.
                for (final int asIndex : addressList) {
                    index.writeLong(bucketAddressOffsetInIndexFile(asIndex), newBucketAddress);
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
                redistributeKeys(bucketAddress);
            }
            //todo remove recursions.
            put(key, value);
        } else {
            final int blockSize = KVP_TOTAL_SIZE;
            if (blockSize > SIZE_OF_RECORD) {
                throw new IllegalArgumentException(blockSize + " is bigger than allowed record size "
                        + SIZE_OF_RECORD);
            }
            long tmpBucketAddress = bucketAddress;
            tmpBucketAddress += 8;
            for (int i = 0; i < bucketElementsCount; i++) {
                int keyLength = data.getInt(tmpBucketAddress);
                tmpBucketAddress += (keyLength + 4);
                int valueLength = data.getInt(tmpBucketAddress);
                tmpBucketAddress += (valueLength + 4);
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
        }
    }

    private void split(int slot) {
        globalDepth++;
        //double index file by copy.
        final int numberOfSlots = (int) Math.pow(2, globalDepth);
        for (int i = 0; i < numberOfSlots / 2; i++) {
            final int hash = index.getInt((i * 12L) + 8);
            final long address = index.getLong(bucketAddressOffsetInIndexFile(i));
            final int siblingSlot = globalDepth == 1 ? 1 : Integer.valueOf("1" + toBinary(hash, globalDepth - 1), 2);
            index.writeInt((siblingSlot * 12L) + 8, siblingSlot);
            if (hash == slot) {
                //old bucket
                data.writeInt(address, globalDepth);
                //new buckets  depth
                final long newBucketsAddress = newAddress(siblingSlot);
                data.writeInt(newBucketsAddress, globalDepth);
                //new buckets  number of records.
                data.writeInt(newBucketsAddress + 4, 0);
                //update index file.
                index.writeLong(bucketAddressOffsetInIndexFile(siblingSlot), newBucketsAddress);
            } else {
                //if no need to create bucket, just point old bucket.
                index.writeLong(bucketAddressOffsetInIndexFile(siblingSlot), address);
            }
        }
        //update index file.
        index.writeInt(0L, globalDepth);
        int totCount = index.getInt(4L);
        totCount -= NUMBER_OF_RECORDS;
        index.writeInt(4L, totCount);
    }

    private int findSlot(Data key, int depth) {
        if (depth == 0) {
            return 0;
        }
        long hash = HASHER.hash(key);

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

    private long newAddress(int slot) {
        return 1L * slot * BUCKET_LENGTH;
    }

    private Data[][] getKeyValuePairs(final long bucketStartOffset) {
        final int numberOfElements = data.getInt(bucketStartOffset + 4);
        final Data[][] dataObjects = new Data[numberOfElements][2];
        long tmpBucketOffset = bucketStartOffset;
        tmpBucketOffset += 8L;
        for (int i = 0; i < numberOfElements; i++) {
            int keyLen = data.getInt(tmpBucketOffset);
            byte[] bytes = new byte[keyLen];
            tmpBucketOffset += 4L;
            data.getBytes(tmpBucketOffset, bytes);
            tmpBucketOffset += keyLen;
            dataObjects[i][0] = new Data(0, bytes);
            int recordLen = data.getInt(tmpBucketOffset);
            bytes = new byte[recordLen];
            tmpBucketOffset += 4L;
            data.getBytes(tmpBucketOffset, bytes);
            tmpBucketOffset += recordLen;
            dataObjects[i][1] = new Data(0, bytes);
        }
        //reset number of elements in this bucket.
        data.writeInt(bucketStartOffset + 4L, 0);
        return dataObjects;
    }


    private void redistributeKeys(long address) {
        final Data[][] keyValuePairs = getKeyValuePairs(address);
        for (int i = 0; i < keyValuePairs.length; i++) {
            put(keyValuePairs[i][0], keyValuePairs[i][1]);
        }
    }

    private long bucketAddressOffsetInIndexFile(int slot) {
        return (slot * 12L) + 8 + 4;
    }

    @Override
    public void close() throws IOException {
        index.close();
        data.close();
    }
}
