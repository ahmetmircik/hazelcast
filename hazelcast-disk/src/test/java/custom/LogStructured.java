//package custom;
//
//import com.hazelcast.disk.Storage;
//import com.hazelcast.disk.core.Hasher;
//import com.hazelcast.disk.core.MappedView;
//import com.hazelcast.nio.serialization.Data;
//
//import java.io.IOException;
//import java.util.*;
//
///**
// * @author: ahmetmircik
// * Date: 1/7/14
// */
//public class LogStructured {
//
//    private final String path;
//    private final Storage data;
//    private final Storage index;
//
//    private static final Hasher<Data, Integer> HASHER = Hasher.DATA_HASHER;
//
//    public LogStructured(String path) {
//        this.path = path;
//        data = new MappedView(this.path + ".data", 1 << 30);
//        index = new MappedView(this.path + ".index", 1 << 4);
//    }
//
//    private long dataPos = 0;
//
//    private long indexPos = 0;
//
//    private int depth;
//
//    private Map<Data, RecordMetadata> map = new HashMap<Data, RecordMetadata>();
//
//    // file-id | address |
//    private static final int BLOCK_SIZE = 1 + 8 + 8;
//    private static final int BUCKET_SIZE = 2;
//
//
//    public Data put(Data key, Data value) {
//        final long startAddress = dataPos;
//
//        data.writeInt(dataPos, key.getBuffer().length);
//        dataPos += 4;
//        data.writeInt(dataPos, value.getBuffer().length);
//        dataPos += 4;
//        data.writeBytes(dataPos, key.getBuffer());
//        dataPos += key.getBuffer().length;
//        data.writeBytes(dataPos, value.getBuffer());
//        dataPos += value.getBuffer().length;
//
//
//        // map.put(key, new RecordMetadata(startAddress, dataPos - startAddress));
//
//        final int slot = findSlot(key, depth);
//        final long start = slot * BLOCK_SIZE * 1L;
//
//
//        final Deque<RecordMetadata> deque = new ArrayDeque<RecordMetadata>();
//
//        byte b = index.getByte(start);
//        int bucketSize = ((int) b);
//        if (bucketSize == BUCKET_SIZE) {
//            depth++;
//            //todo write depth to index file
//            deque.offer(new RecordMetadata(key,data.flush()));
//
//
//
//
//
//        }
//        else {
//            bucketSize++;
//            final long startInBucket = start + (bucketSize * BLOCK_SIZE);
//            index.writeByte(start, (byte) bucketSize);
//            index.writeLong(startInBucket + 1, data.flush());
//            index.writeLong(startInBucket + 8, startAddress);
//        }
//
//        return null;
//    }
//
//    public Data get(Data key) {
//
//        final int slot = findSlot(key, depth);
//        final long start = slot * BLOCK_SIZE * 1L;
//
//        byte b = index.getByte(start);
//        int bucketSize = ((int) b);
//        final long startInBucket = start + (bucketSize * BLOCK_SIZE);
//        final long fileID = index.getLong(startInBucket + 1);
//        final long dataFileStart = index.getLong(startInBucket + 8);
//
//        final long startAddress = dataFileStart;
//
//        final int keyLen = data.getInt(startAddress);
//        final int valueLen = data.getInt(startAddress + 4);
//
//        final byte[] keyBytes = new byte[keyLen];
//        data.getBytes(startAddress + 8, keyBytes);
//
//        final byte[] valueBytes = new byte[valueLen];
//        data.getBytes(startAddress + 8 + keyLen, valueBytes);
//
//        return new Data(0, valueBytes);
//    }
//
////    public void addBucketContent(long bucketStartAddress, Deque<RecordMetadata> deque) {
////
////
////        byte b = index.getByte(bucketStartAddress);
////        final int bucketSize = ((int) b);
////        for (int i = 0; i < bucketSize; i++) {
////            final long fileID = index.getLong(bucketStartAddress + 1);
////            final long dataFileStart = index.getLong(bucketStartAddress + 8);
////
////            deque.add(new RecordMetadata(ke))
////        }
////
////
////        final long startInBucket = start + (bucketSize * BLOCK_SIZE);
////
////        final long startAddress = dataFileStart;
////
////        final int keyLen = data.getInt(startAddress);
////        final int valueLen = data.getInt(startAddress + 4);
////
////        final byte[] keyBytes = new byte[keyLen];
////        data.getBytes(startAddress + 8, keyBytes);
////
////        final byte[] valueBytes = new byte[valueLen];
////        data.getBytes(startAddress + 8 + keyLen, valueBytes);
////
////        return new Data(0, valueBytes);
////    }
//
//    public void close() {
//        try {
//            data.close();
//            index.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    private int findSlot(Data key, int depth) {
//        if (depth == 0) {
//            return 0;
//        }
//        final int hash = HASHER.hash(key);
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
//    }
//
//
//    public class RecordMetadata {
//
//        private Data key;
//        private long startAddress;
//        private long fileID;
//
//        public RecordMetadata(Data key, long fileID) {
//            this.key = key;
//            this.fileID = fileID;
//        }
//
//        public long getStartAddress() {
//            return startAddress;
//        }
//
//        public long getFileID() {
//            return fileID;
//        }
//
//        public Data getKey() {
//            return key;
//        }
//    }
//}
