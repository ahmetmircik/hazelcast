package com.hazelcast.disk.core;

import com.hazelcast.nio.serialization.Data;
import sun.nio.ch.DirectBuffer;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

class Directory implements Closeable {
    private static final String INDEX_FILE_EXTENSION = ".index";
    private static final String DATA_FILE_EXTENSION = ".data";
    int entryCount;
    int depth;
    int keyCountInBucket;

    int entryBlockSize = 1 << 10; // 1K;

    long dataFilePosition;
    long dataFileBlockSize;

    String path;

    RandomAccessFile indexFile;
    FileChannel indexFileChannel;

    RandomAccessFile dataFile;
    FileChannel dataFileChannel;

    MappedByteBuffer bucketLookUp;

    long[] bucketAddress;
    int bucketBlockSize;

    public Directory(String path) throws IOException {
        this.path = path;
        this.depth = 0;
        this.entryCount = 2 ^ depth;
        this.keyCountInBucket = 128;
        this.bucketAddress = new long[entryCount];
//        this.bucketBlockSize =

        createFiles();
        loadBucketLookUp();
    }

    void createFiles() throws IOException {
        // index file.
        this.indexFile = new RandomAccessFile(path + ".index", "rw");
        this.indexFileChannel = indexFile.getChannel();
        // data file.
        this.dataFile = new RandomAccessFile(path + ".data", "rw");
        this.dataFileChannel = dataFile.getChannel();

    }

    void testWrite() throws IOException {

        final long fileChannelSize = 100L;

        for (int i = 0; i < fileChannelSize; i += entryBlockSize) {
            final MappedByteBuffer map = indexFileChannel.map(FileChannel.MapMode.READ_WRITE, i, entryBlockSize);
            final ByteBuffer byteBuffer = map.putLong(i);
            System.out.println(((DirectBuffer) byteBuffer).address());
        }

    }

    long[] loadBucketLookUp() throws IOException {
        final long fileChannelSize = indexFileChannel.size();
        // initially one room to locate.
        if (fileChannelSize <= 0) {
            bucketAddress[0] = 0;
            return bucketAddress;
        }
        // todo do not load all index file
        bucketLookUp = indexFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileChannelSize);
        bucketLookUp.order(ByteOrder.nativeOrder());
        final LongBuffer longBuffer = bucketLookUp.asLongBuffer();
        bucketAddress = longBuffer.array();
        return bucketAddress;
    }

    public void insert(Data key, Data value) throws IOException {
        final long address = makeAddress(key, depth);
        final long size = dataFileChannel.size();
        final MappedByteBuffer map = dataFileChannel.map(FileChannel.MapMode.READ_WRITE,
                bucketAddress[(int)address], entryBlockSize * keyCountInBucket);


    }
    // todo murmurhash3??ba
    private static final Hasher<Data, Integer> HASHER = new Hasher<Data, Integer>() {
        @Override
        public Integer hash(Data s) {
            return s.hashCode() % 19997;
        }
    };


    long makeAddress(Data key, int depth) {
        if (depth == 0) {
            return 0;
        }
        final long hash = HASHER.hash(key);
        return Long.parseLong(Long.toBinaryString(hash).substring(0, depth), 2);
    }


    @Override
    public void close() throws IOException {
        dataFile.close();
        indexFile.close();
    }
}