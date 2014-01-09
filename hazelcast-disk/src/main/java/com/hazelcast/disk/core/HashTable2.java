package com.hazelcast.disk.core;

import com.hazelcast.disk.PersistencyUnit;
import com.hazelcast.disk.Storage;
import com.hazelcast.disk.Utils;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;
import java.util.Arrays;
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
 * Position : 8 --> address (64 bit)
 * Position : 16 -->address (64 bit)
 * Position : 24 -->address (64 bit)
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
public class HashTable2 extends PersistencyUnit {

    private static final Hasher<Data, Integer> HASHER = Hasher.DATA_HASHER;
    private static final int NUMBER_OF_RECORDS = 16;
    private static final int KVP_TOTAL_SIZE = 16 + 512;//KVP
    private static final int SIZE_OF_RECORD = 8 + KVP_TOTAL_SIZE;
    private static final int BUCKET_LENGTH = Utils.next2(4 + 4 + (NUMBER_OF_RECORDS * SIZE_OF_RECORD));
    private static final int INDEX_BLOCK_LENGTH = (int) Math.pow(2, 10);
    private static final int DATA_BLOCK_LENGTH = (int) Math.pow(2, 16);


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

    long lastBucketPosition = 0;

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
        else {
            lastBucketPosition = data.size();
        }
    }

    @Override
    public Data put(Data keyIn, Data valueIn) {
        //todo what is most appropriate data structure here?
        final Deque<Data> stack = new LinkedList<Data>();
        stack.offerFirst(valueIn);
        stack.offerFirst(keyIn);

        while (true) {

            Data key = stack.peekFirst();
            if (key == null) break;

            final int slot = findSlot(key, globalDepth);
            final long bucketAddress = index.getLong(bucketAddressOffsetInIndexFile(slot));
            int bucketDepth = data.getInt(bucketAddress);
            int bucketElementsCount = data.getInt(bucketAddress + 4L);

            if (bucketElementsCount == NUMBER_OF_RECORDS) {
                if (bucketDepth < globalDepth) {
                    final int[] addressList = newRange2(slot, bucketDepth);
                    ++bucketDepth;
                    //write buckets new depth.
                    data.writeInt(bucketAddress, bucketDepth);
                    // create new bucket.
                    final long newBucketAddress = createNewBucketAddress();
                    data.writeInt(newBucketAddress, bucketDepth);
                    data.writeInt(newBucketAddress + 4L, 0); //number of records.
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
                tmpBucketAddress += 8L;
                for (int i = 0; i < bucketElementsCount; i++) {
                    int keyLength = data.getInt(tmpBucketAddress);
                    tmpBucketAddress += (keyLength + 4L);
                    int valueLength = data.getInt(tmpBucketAddress);
                    tmpBucketAddress += (valueLength + 4L);
                }
                final int keyLength = key.getBuffer().length;
                data.writeInt(tmpBucketAddress, keyLength);
                tmpBucketAddress += 4L;
                data.writeBytes(tmpBucketAddress, key.getBuffer());
                tmpBucketAddress += keyLength;
                Data keyTmp = stack.pollFirst();
                Data value = stack.pollFirst();
                final int valueLength = value.getBuffer().length;
                data.writeInt(tmpBucketAddress, valueLength);
                tmpBucketAddress += 4L;
                data.writeBytes(tmpBucketAddress, value.getBuffer());
                data.writeInt(bucketAddress + 4L, ++bucketElementsCount);

                totalCount += 1;
            }
        }

        // todo should return previous???
        return null;
    }


    @Override
    public Data get(Data key) {
        final int slot = findSlot(key, globalDepth);
        long address = index.getLong(bucketAddressOffsetInIndexFile(slot));
        final int bucketSize = data.getInt(address += 4);
        address += 4;
        for (int j = 0; j < bucketSize; j++) {
            final int keyLen = data.getInt(address);
            byte[] arr = new byte[keyLen];
            data.getBytes(address += 4, arr);
            final Data keyRead = new Data(0, arr);
            final int recordLen = data.getInt(address += keyLen);
            if(key.equals(keyRead)){
                arr = new byte[recordLen];
                data.getBytes(address += 4, arr);
                final Data valueRead = new Data(0, arr);
                address += recordLen;
                return valueRead;
            }
            else {
                address += 4L;
                address += recordLen;

            }

        }
        return null;
    }

    //todo
    @Override
    public Data remove(Data key) {
        final int slot = findSlot(key, globalDepth);
        long address = index.getLong(bucketAddressOffsetInIndexFile(slot));
        final int bucketSize = data.getInt(address += 4);
        address += 4L;
        for (int j = 0; j < bucketSize; j++) {
            final int keyLen = data.getInt(address);
            byte[] arr = new byte[keyLen];
            data.getBytes(address += 4, arr);
            final Data keyRead = new Data(0, arr);
            final int recordLen = data.getInt(address += keyLen);
            if(key.equals(keyRead)){
                arr = new byte[recordLen];
                data.getBytes(address += 4, arr);
                final Data valueRead = new Data(0, arr);
                address += recordLen;
                return valueRead;
            }
            else {
                address += 4L;
                address += recordLen;

            }
        }
        return null;
    }

    @Override
    public void flush() {
        data.flush();
        index.flush();
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

    private void split(int slot) {
        //inc depth.
        globalDepth++;
        //double index file by copying.
        final int numberOfSlots = (int) Math.pow(2, globalDepth - 1);
        for (int i = 0; i < numberOfSlots; i++) {
            final long address = index.getLong(bucketAddressOffsetInIndexFile(i));
            final int siblingSlot = i + numberOfSlots;
            final long bucketAddressOffsetInIndexFile = bucketAddressOffsetInIndexFile(siblingSlot);
            if (i == slot) {
                //old bucket
                data.writeInt(address, globalDepth);
                //new buckets  depth
                final long newBucketsAddress = createNewBucketAddress();
                data.writeInt(newBucketsAddress, globalDepth);
                //new buckets  number of records.
                data.writeInt(newBucketsAddress + 4L, 0);
                //update index file.
                index.writeLong(bucketAddressOffsetInIndexFile, newBucketsAddress);
            } else {
                //if no need to create bucket, just point old bucket.
                index.writeLong(bucketAddressOffsetInIndexFile, address);
            }
        }
    }

    //todo what if depth > 31?
    private int findSlot(Data key, int depth) {
        if (depth > 31) throw new IllegalArgumentException("depth is not supported\t:" + depth);
        if (depth == 0) {
            return 0;
        }
        final int hash = HASHER.hash(key);
        return ((hash & (0xFFFFFFFF >>> (32 - depth))));

    }


    private int[] newRange2(int index, int bucketDepth) {
        ++bucketDepth;
        int lastNBits = ((index & (0xFFFFFFFF >>> (32 - (bucketDepth)))));
        lastNBits |= (int) Math.pow(2, bucketDepth - 1);

        int[] x = new int[(int) Math.pow(2, globalDepth - bucketDepth)];
        for (int i = 0; i < x.length; i++) {
            x[i] = -1;
        }
        x[0] = lastNBits;

        while (globalDepth - bucketDepth > 0) {
            final int mask = (int) Math.pow(2, bucketDepth);
            final int pow = (int) Math.pow(2, globalDepth - bucketDepth - 1);
            for (int i = 0; i < x.length; i+=pow) {
                if (x[i] == -1) continue;
                final Integer param = x[i];
                x[i] = (param | mask);
                x[i+=pow] = (param & ~mask);
            }

            bucketDepth++;
        }
        Arrays.sort(x);
        return x;
    }


    private int[] newRange0(int index, final int bucketDepth) {
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

    private  int[] newRange1(int index, int bucketDepth) {
        ++bucketDepth;
        int lastNBits = ((index & (0xFFFFFFFF >>> (32 - (bucketDepth)))));
        lastNBits |= (int) Math.pow(2, bucketDepth - 1);

        int[] x = new int[(int) Math.pow(2, globalDepth - bucketDepth)];
        for (int i = 0; i < x.length; i++) {
            x[i] = -1;
        }
        x[0] = lastNBits;

        while (globalDepth - bucketDepth > 0) {
            final int mask = (int) Math.pow(2, bucketDepth);
            for (int i = 0; i < x.length; i ++) {
                if (x[i] == -1) continue;
                final Integer param = x[i];
                x[i] = (param | mask);
                int g = 0;
                for (int j = 0; j < x.length; j++) {
                   if(x[j] == -1){
                      g =j;
                   }
                }
                x[g] = (param & ~mask);
            }
            bucketDepth++;
        }
        Arrays.sort(x);
        return x;
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

    private long createNewBucketAddress() {
        return lastBucketPosition += BUCKET_LENGTH;
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
        return (slot * 8L) + 8L;
    }

}
