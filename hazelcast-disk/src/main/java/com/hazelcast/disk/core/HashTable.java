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
    public static final int BLOCK_SIZE = 16;//KVP
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
        data = new MappedView(path + ".data", 1 << 30);
        index = new MappedView(path + ".index", 1 << 8);
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

    private long bucketAddressOffsetInIndexFile(int slot) {
        return (slot * 12L) + 8 + 4;
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
            } else {
                split(slot);
                redistributeKeys(bucketAddress);
            }
            //todo remove recursions.
            put(key, value);
        } else {
            final int blockSize = BLOCK_SIZE;
            if (blockSize > SIZE_OF_RECORD) {
                throw new IllegalArgumentException(blockSize + " is bigger than allowed record size "
                        + SIZE_OF_RECORD);
            }
            long tmpBucketAddress = bucketAddress;
            tmpBucketAddress += 8;
            for (int i = 0; i < bucketElementsCount; i++) {
                //todo fix this
                tmpBucketAddress += 2 * 8 + 8;
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

            //logRecord(logAddress, "p1 ("+bucketElementsCount + ") ("+key.hashCode()+") ");
        }
    }

    void logRecord(long address, String x) {
        final int keyLen = data.getInt(address);
        address += keyLen;
        address += 4;
        final int recordLen = data.getInt(address);
        System.out.println(x + "log record\t" + keyLen + "---" + recordLen + "[" + address + "]");

    }

    // 20640
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
                data.writeInt(newBucketsAddress + 4L, 0);
                //update index file.
                index.writeLong(bucketAddressOffsetInIndexFile(siblingSlot), newBucketsAddress);
            } else {
                //if no need to create bucket, just point old bucket.
                index.writeLong(bucketAddressOffsetInIndexFile(siblingSlot), address);
            }

//            System.out.println(hash + "--" + address + " | " + siblingSlot + "--" + newAddress(siblingSlot));
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
        tmpBucketOffset += 8;
        for (int i = 0; i < numberOfElements; i++) {
            final int keyLen = data.getInt(tmpBucketOffset);
            if(keyLen<0){
                System.out.println("keylen");
            }
            byte[] bytes = new byte[keyLen];
            tmpBucketOffset += 4;
            data.getBytes(tmpBucketOffset, bytes);
            tmpBucketOffset += keyLen;
            dataObjects[i][0] = new Data(0, bytes);
            final int recordLen = data.getInt(tmpBucketOffset);
            if(recordLen<0){
                System.out.println("keylen");
            }
            bytes = new byte[recordLen];
            tmpBucketOffset += 4;
            data.getBytes(tmpBucketOffset, bytes);
            tmpBucketOffset += recordLen;
            dataObjects[i][1] = new Data(0, bytes);
        }
        //reset number of elements in this bucket.
        data.writeInt(bucketStartOffset + 4, 0);
        return dataObjects;
    }


    private void redistributeKeys(long address) {
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
