package com.hazelcast.disk.core;

import com.hazelcast.disk.PersistencyUnit;
import com.hazelcast.disk.Storage;
import com.hazelcast.disk.helper.Utils;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;
import java.util.*;

/**
 * <p/>
 * <p/>
 * <p/>
 * IndexFile Format
 * -------------------------
 * Position : 0  --> depth
 * Position : 4  --> number of records
 * Position : 12 --> address (64 bit)
 * Position : 16 -->address (64 bit)
 * Position : 24 -->address (64 bit)
 * ...
 * ...
 * ...
 * <p/>
 * DataFile Format
 * -------------------------
 * Position : 0 * BUCKET_LENGTH --> 1st Bucket : bucket depth (32 bit) & number of records (32 bit) & [record header (1 byte) + KVP] & [record header (1 byte) + KVP]...
 * Position : 1 * BUCKET_LENGTH --> 2nd Bucket : bucket depth (32 bit) & number of records (32 bit) & [record header (1 byte) + KVP] & [record header (1 byte) + KVP]...
 * Position : 2 * BUCKET_LENGTH --> 3rd Bucket : bucket depth (32 bit) & number of records (32 bit) & [record header (1 byte) + KVP] & [record header (1 byte) + KVP]...
 * ...
 * ...
 * ...
 * <p/>
 * Record Format
 * --------------------------
 * HEADER + KEY LENGTH + VALUE LENGTH + KEY + VALUE
 */
public final class HashTable extends PersistencyUnit {

    private static final Hasher<Data, Integer> HASHER = Hasher.DATA_HASHER;
    private static final int NUMBER_OF_RECORDS = 32;
    private static final int KVP_TOTAL_SIZE = 32 + 10 * 1024;//KVP
    private static final int ONE_RECORD_HEADER_SIZE = 1;
    private static final int SIZE_OF_RECORD = ONE_RECORD_HEADER_SIZE + 8 + KVP_TOTAL_SIZE;
    private static final int BUCKET_LENGTH = Utils.next2(4 + 4 + (NUMBER_OF_RECORDS * SIZE_OF_RECORD));
    private static final int INDEX_BLOCK_LENGTH = (int) Math.pow(2, 16);//64KB
    private static final int DATA_BLOCK_LENGTH = (int) Math.pow(2, 22);//4MB
    private static final int MARK_REMOVED = 0x2;
    private static final Data[][] EMPTY_KVP = new Data[0][2];
    private final String path;
    private final Storage data;
    private final Storage index;
    private long totalCount = 0L;
    private int globalDepth;
    private long lastBucketPosition = 0;


    //todo path length limit OS.
    public HashTable(String path) {
        if (path == null || path.length() == 0)
            throw new NullPointerException();
        this.path = path;
        data = new MappedView(this.path + ".data", DATA_BLOCK_LENGTH);
        index = new MappedView(this.path + ".index", INDEX_BLOCK_LENGTH);
        lastBucketPosition = data.size();
        globalDepth = index.getInt(0);
        totalCount = index.getLong(4);
        if (globalDepth == 0 && totalCount == 0) {
            index.writeInt(0, globalDepth);  //depth
        }
        log("BUCKET_LENGTH\t" + BUCKET_LENGTH, true);
    }

    //todo should return previous value???
    //todo put big data with linked records?
    //todo prevent dublicate key put??
    @Override
    public Data put(Data keyIn, Data valueIn) {
        if (keyIn == null)
            throw new NullPointerException();
        if (valueIn == null)
            throw new NullPointerException();
        //todo what is most appropriate data structure here?
        final Deque<Data> stack = new LinkedList<Data>();
        stack.offerFirst(valueIn);
        stack.offerFirst(keyIn);

        while (true) {
            final Data key = stack.peekFirst();
            if (key == null) break;
            final int slot = findSlot(key, globalDepth);
            final long bucketAddress = index.getLong(bucketAddressOffsetInIndexFile(slot));
            int bucketDepth = data.getInt(bucketAddress);
            int bucketElementsCount = data.getInt(bucketAddress + 4L);
            if (bucketElementsCount == NUMBER_OF_RECORDS) {
                if (bucketDepth < globalDepth) {
                    log("if (bucketDepth < globalDepth)");
                    final int[] addressList = newRange(slot, bucketDepth);
                    ++bucketDepth;
                    //write buckets new depth.
                    data.writeInt(bucketAddress, bucketDepth);
                    // create new bucket.
                    final long newBucketAddress = createNewBucketAddress();
                    data.writeInt(newBucketAddress, bucketDepth);
                    //update buddies.
                    for (final int asIndex : addressList) {
                        index.writeLong(bucketAddressOffsetInIndexFile(asIndex), newBucketAddress);
                    }
                } else {
                    log("double index size");
                    globalDepth++;
                    //double index file by copying.
                    final int numberOfSlots = (int) Math.pow(2, globalDepth - 1);
                    for (int i = 0; i < numberOfSlots; i++) {
                        final long address = index.getLong(bucketAddressOffsetInIndexFile(i));
                        final int siblingSlot = i + numberOfSlots;
                        final long bucketAddressPosition = bucketAddressOffsetInIndexFile(siblingSlot);
                        index.writeLong(bucketAddressPosition, address);
                    }
                }
                final Data[][] keyValuePairs = getKeyValuePairs(bucketAddress);
                for (int i = 0; i < keyValuePairs.length; i++) {
                    stack.offerFirst(keyValuePairs[i][1]);
                    stack.offerFirst(keyValuePairs[i][0]);
                }
                //reset number of elements in this bucket.
                data.writeInt(bucketAddress + 4L, 0);
                totalCount -= keyValuePairs.length;
            } else {
                //insert KVP.
                Data keyTmp = stack.pollFirst(); //dummy poll.
                Data value = stack.pollFirst();
                final int blockSize = KVP_TOTAL_SIZE;
                if (blockSize > SIZE_OF_RECORD) {
                    throw new IllegalArgumentException(blockSize + " is bigger than allowed record size "
                            + SIZE_OF_RECORD);
                }
                //new puts key-value lengths.
                final int keyLengthIn = key.getBuffer().length;
                final int valueLengthIn = value.getBuffer().length;
                long currentBucketLength = 8L;
                long tmpBucketAddress = bucketAddress;
                tmpBucketAddress += 8L;
                for (int i = 0; i < bucketElementsCount; ) {
                    final long bucketBaseAddress = tmpBucketAddress;
                    final byte header = data.getByte(tmpBucketAddress);
                    final boolean isRemovedRecord = header == MARK_REMOVED;
                    tmpBucketAddress += ONE_RECORD_HEADER_SIZE;//record header
                    final int keyLen = data.getInt(tmpBucketAddress);
                    tmpBucketAddress += 4L;
                    final int valueLen = data.getInt(tmpBucketAddress);
                    tmpBucketAddress += (keyLen + valueLen + 4L);
                    if (!isRemovedRecord) {
                        i++;
                        currentBucketLength += keyLen + valueLen + 8L;
                    } else {
                        // we can write on removed record.
                        if (keyLengthIn + valueLengthIn <= keyLen + valueLen) {
                            tmpBucketAddress = bucketBaseAddress;
                            break;
                        }
                    }
                }
                if (BUCKET_LENGTH - currentBucketLength < keyLengthIn + valueLengthIn + 8L) {
                    throw new NotEnoughSpaceException("No space left for the record in bucket");
                }
                tmpBucketAddress += ONE_RECORD_HEADER_SIZE;//header
                data.writeInt(tmpBucketAddress, keyLengthIn);
                tmpBucketAddress += 4L;
                data.writeInt(tmpBucketAddress, valueLengthIn);
                tmpBucketAddress += 4L;
                data.writeBytes(tmpBucketAddress, key.getBuffer());
                tmpBucketAddress += keyLengthIn;
                data.writeBytes(tmpBucketAddress, value.getBuffer());
                data.writeInt(bucketAddress + 4L, ++bucketElementsCount);
                totalCount++;
            }
        }
        return null;
    }


    @Override
    public Data get(Data key) {
        if (key == null)
            throw new NullPointerException();
        final int slot = findSlot(key, globalDepth);
        long address = index.getLong(bucketAddressOffsetInIndexFile(slot));
        address += 4L;//bucket depth.
        final int bucketSize = data.getInt(address);
        address += 4;
        for (int j = 0; j < bucketSize; ) {
            final byte header = data.getByte(address);
            final boolean isRemmovedRecord = header == MARK_REMOVED;
            address += ONE_RECORD_HEADER_SIZE;//header
            final int keyLen = data.getInt(address);
            address += 4L;
            final int recordLen = data.getInt(address);
            address += 4L;
            byte[] arr = new byte[keyLen];
            data.getBytes(address, arr);
            final Data keyRead = new Data(0, arr);
            address += keyLen;
            if (key.equals(keyRead)) {
                if (isRemmovedRecord) {
                    // already removed record.
                    return null;
                }
                arr = new byte[recordLen];
                data.getBytes(address, arr);
                final Data valueRead = new Data(0, arr);
                return valueRead;
            } else {
                address += recordLen;
            }

            if (!isRemmovedRecord) {
                j++;
            }

        }
        return null;
    }

    //todo cacheline.
    @Override
    public Data remove(Data key) {
        if (key == null)
            throw new NullPointerException();
        final int slot = findSlot(key, globalDepth);
        final long bucketBaseAddress = index.getLong(bucketAddressOffsetInIndexFile(slot));
        long address = bucketBaseAddress;
        final int bucketSize = data.getInt(address += 4);
        address += 4L;
        for (int j = 0; j < bucketSize; ) {
            final long recordBaseAddress = address;
            final byte header = data.getByte(address);
            boolean isRemovedRecord = header == MARK_REMOVED;
            address += ONE_RECORD_HEADER_SIZE;
            final int keyLen = data.getInt(address);
            address += 4L;
            final int recordLen = data.getInt(address);
            address += 4L;
            byte[] keyBuffer = new byte[keyLen];
            data.getBytes(address, keyBuffer);
            address += keyLen;
            if (Arrays.equals(keyBuffer, key.getBuffer())) {
                if (isRemovedRecord) {
                    // already removed record.
                    return null;
                } else {
                    //mark as removed.
                    data.writeByte(recordBaseAddress, (byte) (header | MARK_REMOVED));
                    data.writeInt(bucketBaseAddress + 4L, bucketSize - 1);
                    totalCount--;
                    //read removed record.
                    byte[] recordBuffer = new byte[recordLen];
                    data.getBytes(address, recordBuffer);
                    final Data valueRead = new Data(0, recordBuffer);
                    //retur old value.
                    return valueRead;
                }
            } else {
                address += recordLen;
            }
            if (!isRemovedRecord) {
                j++;
            }
        }
        // no record found return null;
        return null;
    }

    @Override
    public void flush() {
        data.flush();
        index.flush();
    }

    @Override
    public long size() {
        return totalCount;
    }

    @Override
    public void close() throws IOException {
        index.writeInt(0L, globalDepth);
        index.writeLong(4L, totalCount);

        log(totalCount, true);

        data.close();
        index.close();
    }

    public Map<Data, Data> readSequentially() throws IOException {
        final HashMap<Data, Data> dataDataHashMap = new HashMap<Data, Data>();
        long size = data.size();
        for (int i = 0; i < size / HashTable.BUCKET_LENGTH; i++) {
            long address = 1L * i * HashTable.BUCKET_LENGTH;
            final int bucketDepth = data.getInt(address);
            address += 4L;
            final int bucketSize = data.getInt(address);
            address += 4L;
            for (int j = 0; j < bucketSize; ) {
                final byte header = data.getByte(address);
                final boolean isRemmovedRecord = header == MARK_REMOVED;
                address += ONE_RECORD_HEADER_SIZE;//header
                final int keyLen = data.getInt(address);
                address += 4L;
                final int recordLen = data.getInt(address);
                address += 4L;
                byte[] arr = new byte[keyLen];
                data.getBytes(address, arr);
                final Data keyRead = new Data(0, arr);
                address += keyLen;
                arr = new byte[recordLen];
                data.getBytes(address, arr);
                final Data valueRead = new Data(0, arr);
                address += recordLen;
                if (!isRemmovedRecord) {
                    dataDataHashMap.put(keyRead, valueRead);
                    j++;
                }
            }
        }
        return dataDataHashMap;
    }

    //todo what if depth > 31?
    private int findSlot(Data key, int depth) {
        if (depth > 31) throw new IllegalArgumentException("depth is not supported\t:" + depth);
        if (depth == 0) {
            return 0;
        }
        final int hash = HASHER.hash(key);
        return ((hash & (0x7FFFFFFF >>> (32 - depth))));

    }

    private int[] newRange(int index, int bucketDepth) {
        ++bucketDepth;
        int lastNBits = ((index & (0x7FFFFFFF >>> (32 - (bucketDepth)))));
        lastNBits |= (int) Math.pow(2, bucketDepth - 1);

        int[] x = new int[(int) Math.pow(2, globalDepth - bucketDepth)];
        for (int i = 0; i < x.length; i++) {
            x[i] = -1;
        }
        x[0] = lastNBits;

        while (globalDepth - bucketDepth > 0) {
            final int mask = (int) Math.pow(2, bucketDepth);
            final int pow = (int) Math.pow(2, globalDepth - bucketDepth - 1);
            for (int i = 0; i < x.length; i += pow) {
                if (x[i] == -1) continue;
                final Integer param = x[i];
                x[i] = (param | mask);
                x[i += pow] = (param & ~mask);
            }

            bucketDepth++;
        }
        Arrays.sort(x);
        return x;
    }


    private long createNewBucketAddress() {
        return lastBucketPosition += (BUCKET_LENGTH * 1L);
    }

    private Data[][] getKeyValuePairs(final long bucketStartOffset) {
        final int numberOfElements = data.getInt(bucketStartOffset + 4);
        if (numberOfElements == 0) {
            return EMPTY_KVP;
        }
        final Data[][] dataObjects = new Data[numberOfElements][2];
        long tmpBucketOffset = bucketStartOffset;
        tmpBucketOffset += 8L;
        for (int i = 0; i < numberOfElements; ) {
            final byte header = data.getByte(tmpBucketOffset);
            tmpBucketOffset += ONE_RECORD_HEADER_SIZE;
            final int keyLen = data.getInt(tmpBucketOffset);
            tmpBucketOffset += 4L;
            final int recordLen = data.getInt(tmpBucketOffset);
            tmpBucketOffset += 4L;
            //read key
            byte[] bytes = new byte[keyLen];
            data.getBytes(tmpBucketOffset, bytes);
            dataObjects[i][0] = new Data(0, bytes);
            tmpBucketOffset += keyLen;
            bytes = new byte[recordLen];
            data.getBytes(tmpBucketOffset, bytes);
            tmpBucketOffset += recordLen;
            //todo improve logic here
            if (header != MARK_REMOVED) {
                dataObjects[i][1] = new Data(0, bytes);
                i++;
            }
        }
        return dataObjects;
    }


    private long bucketAddressOffsetInIndexFile(int slot) {
        return (((long) slot) << 3) + 12L;
    }


    /**
     * ----------- DEBUG METHODS START ----------
     */

    private void printIndexFile() {

        final int numberOfSlots = (int) Math.pow(2, globalDepth);
        int depth = index.getInt(0);
        int count = index.getInt(4);
        log("======================");
        log("depth = " + globalDepth);
        log("count = " + totalCount);
        for (int i = 0; i < numberOfSlots; i++) {
            log("[" + i + "]" + index.getLong(bucketAddressOffsetInIndexFile(i)));
        }
        log("======================");
    }

    private void log(Object str) {

        log(str, false);
    }

    private void log(Object str, boolean open) {
        if (open) {
            System.out.println(str);
        }
    }

    private void printDataFile() {
        long total = 0;
        long size = data.size();
        boolean print = false;
        if (!print) return;
        log("File size\t:" + size + " GD {" + globalDepth + "}", print);
        for (int i = 0; i < size / HashTable.BUCKET_LENGTH; i++) {
            long address = 1L * i * HashTable.BUCKET_LENGTH;
            if (address > lastBucketPosition) break;
            final int bucketDepth = data.getInt(address);
            address += 4L;
            final int bucketSize = data.getInt(address);
//            log(address - 4 + "\t:d[" + bucketDepth + "] s[" + bucketSize + "]", print);
            address += 4L;
            String p = "";
            for (int j = 0; j < bucketSize; ) {
                final byte header = data.getByte(address);
                final boolean isRemoved = header == MARK_REMOVED;
                address += ONE_RECORD_HEADER_SIZE;//header
                final int keyLen = data.getInt(address);
                address += 4L;
                final int recordLen = data.getInt(address);
                address += 4L;
                byte[] arr = new byte[keyLen];
                data.getBytes(address, arr);
                final Data keyRead = new Data(0, arr);
                address += keyLen;
                arr = new byte[recordLen];
                data.getBytes(address, arr);
                final Data valueRead = new Data(0, arr);
                address += recordLen;
                total++;
                p += (total + "-RMV[" + isRemoved + "]S:{" + findSlot(keyRead, globalDepth) + "}"
                        + "K[" + keyRead.hashCode() + "]" + "V[" + valueRead.hashCode() + "]" + " | ");
                if (!isRemoved) {
                    j++;
                }
            }
            log(p, print);
            log("\n-------------------------");
        }
    }

}
