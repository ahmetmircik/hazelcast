//package com.hazelcast.disk.impl;
//
//import com.hazelcast.disk.PersistencyUnit;
//import com.hazelcast.disk.Storage;
//import com.hazelcast.nio.serialization.Data;
//import com.sun.istack.internal.NotNull;
//import sun.misc.Unsafe;
//import sun.nio.ch.DirectBuffer;
//
//import java.io.FileNotFoundException;
//import java.io.IOException;
//import java.io.RandomAccessFile;
//import java.lang.reflect.Field;
//import java.nio.MappedByteBuffer;
//import java.nio.channels.FileChannel;
//
///**
// * @author: ahmetmircik
// * Date: 12/20/13
// */
//class MMappedFile<K, V> implements PersistencyUnit<MappedByteBuffer>, Storage<K, V> {
//
//    @NotNull
//    @SuppressWarnings("ALL")
//    public static final Unsafe UNSAFE;
//    static final int BYTES_OFFSET;
//
//    static {
//        try {
//            @SuppressWarnings("ALL")
//            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
//            theUnsafe.setAccessible(true);
//            UNSAFE = (Unsafe) theUnsafe.get(null);
//            BYTES_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
//            System.out.println("BYTES_OFFSET " + BYTES_OFFSET);
//
//        } catch (Exception e) {
//            throw new AssertionError(e);
//        }
//    }
//
//    private final FileChannel fileChannel;
//
//    private long position;
//
//    private final long size;
//
//    private MappedByteBuffer mappedByteBuffer;
//
//
//    public MMappedFile(String filePath, StorageConfig storageConfig) throws FileNotFoundException {
//        fileChannel = new RandomAccessFile(filePath, "rw").getChannel();
//        size = storageConfig.getBlockSize();
//        position = loadLastPosition();
//    }
//
//    @Override
//    public MappedByteBuffer createNew() throws IOException {
//        mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, position, size);
//        position = ((DirectBuffer) mappedByteBuffer).address();
//        return mappedByteBuffer;
//    }
//
//    // todo
//    public long loadLastPosition() {
//        return 0L;
//    }
//
//    @Override
//    public void put(K key, V value) {
//        if (value instanceof Data) {
//            write(((Data) value).getBuffer(), 0, ((Data) value).getBuffer().length);
//        } else {
//            writeLong((Long) value);
//        }
////
//    }
//
//    public void write(byte[] bytes, int off, int len) {
//        UNSAFE.copyMemory(bytes, BYTES_OFFSET + off, null, position, len);
//        position += len;
//    }
//
//    public void writeLong(long v) {
//        UNSAFE.putLong(position, v);
//        position += 8;
//    }
//
//    public void writeByte(int i) {
//        UNSAFE.putByte(position++, (byte) i);
//    }
//
//    @Override
//    public V get(K key) {
//        return null;
//    }
//
//    @Override
//    public V remove(K key) {
//        return null;
//    }
//
//    @Override
//    public void close() {
//        try {
//            fileChannel.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    @Override
//    public long getPosition() {
//        return position;
//    }
//
//    @Override
//    public long getSize() {
//        try {
//            return fileChannel.size();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return  -1L;
//    }
//}
