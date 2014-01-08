package com.hazelcast.disk.core;

import com.hazelcast.disk.Storage;
import com.hazelcast.nio.serialization.Data;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
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
public class HashTable2 implements Closeable {

    private static final Hasher<Data, Integer> HASHER = Hasher.DATA_HASHER;
    private static final int NUMBER_OF_RECORDS = 20;
    private static final int KVP_TOTAL_SIZE = 512 + 16;//KVP
    private static final int SIZE_OF_RECORD = 8 + KVP_TOTAL_SIZE;
    public static final int BUCKET_LENGTH = 4 + 4 + (NUMBER_OF_RECORDS * SIZE_OF_RECORD);
    public static final int INDEX_BLOCK_LENGTH = (int) Math.pow(2, 10);
    public static final int DATA_BLOCK_LENGTH = (int) Math.pow(2, 30);


    private final String path;
    private final Storage data;
    private final Storage index;
    private int globalDepth;
    private int totalCount = 0;

    public HashTable2(String path) {
        this.path = path;
        data = new MappedView(this.path + ".data", DATA_BLOCK_LENGTH);
        index = new MappedView(this.path + ".index", INDEX_BLOCK_LENGTH);
        init();
        System.out.println("BUCKET_LENGTH\t" + BUCKET_LENGTH);
    }

    private void init() {
        globalDepth = index.getInt(0);
        totalCount = index.getInt(4);
        if (globalDepth == 0) {
            data.writeInt(0, 0);
            data.writeInt(4, 0);
            index.writeInt(0, globalDepth);  //depth
            index.writeInt(4, 0);  //count
            index.writeLong(8, 0L); //address
        }
    }

    public void put(Data ikey, Data ivalue) {

        final Deque<Data> stack = new ArrayDeque<Data>();
        stack.offerFirst(ivalue);
        stack.offerFirst(ikey);

        while (true) {

            Data key = stack.peekFirst();
            if (key == null) break;

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
                    //update buddies.
                    for (final int asIndex : addressList) {
                        index.writeLong(bucketAddressOffsetInIndexFile(asIndex), newBucketAddress);
                    }
                } else {
                    split(slot);
                }

                final Data[][] keyValuePairs = getKeyValuePairs(bucketAddress);
                for (int i = 0; i < keyValuePairs.length; i++) {
                    stack.offerFirst(keyValuePairs[i][1]);
                    stack.offerFirst(keyValuePairs[i][0]);
                }
                //reset number of elements in this bucket.
                data.writeInt(bucketAddress + 4L, 0);
                totalCount -= keyValuePairs.length;
                //insert KVP.
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
                Data keyTmp = stack.pollFirst();
                Data value = stack.pollFirst();
                final int valueLength = value.getBuffer().length;
                data.writeInt(tmpBucketAddress, valueLength);
                tmpBucketAddress += 4;
                data.writeBytes(tmpBucketAddress, value.getBuffer());
                data.writeInt(bucketAddress + 4, ++bucketElementsCount);

                totalCount += 1;
            }
        }
    }

    private void split(int slot) {
        //inc depth.
        globalDepth++;
        //double index file by copying.
        final int numberOfSlots = (int) Math.pow(2, globalDepth - 1);
        for (int i = 0; i < numberOfSlots; i++) {
            final long address = index.getLong(bucketAddressOffsetInIndexFile(i));
            final int siblingSlot = globalDepth == 1 ? 1 : Integer.valueOf("1" + toBinary(i, globalDepth - 1), 2);
            if (i == slot) {
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
    }

    //todo what if depth > 31?
    private int findSlot(Data key, int depth) {
        if(depth > 31) throw new IllegalArgumentException("depth is not supported\t:" + depth);
        if (depth == 0) {
            return 0;
        }
        final int hash = HASHER.hash(key);
        return ((hash & (0xFFFFFFFF >>> (32 - depth))));

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
        return dataObjects;
    }


    private long bucketAddressOffsetInIndexFile(int slot) {
        return (slot * 8L) + 8;
    }

    @Override
    public void close() throws IOException {
        System.out.println("globalDepth\t:" + globalDepth);
        System.out.println("totalCount\t:" + totalCount);
        index.writeInt(0L, globalDepth);
        index.writeInt(4L, totalCount);


        index.close();
        data.close();
    }
}
