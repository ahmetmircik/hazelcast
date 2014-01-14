package com.hazelcast.disk.core;

import com.hazelcast.disk.Storage;
import com.hazelcast.disk.helper.Utils;

import java.io.IOError;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * @author: ahmetmircik
 * Date: 1/1/14
 */
public class MappedView implements Storage {

    private ByteOrder NATIVE = ByteOrder.nativeOrder();
    public final int VIEW_POSITION_SHIFT;
    public final int VIEW_CHUNK_SIZE;
    private final String path;
    private final RandomAccessFile file;
    private final FileChannel fileChannel;
    private String mode;

    private MappedByteBuffer[] views;

    private int blockSize;


    public MappedView(String path, int blockSize) {
        this(path, blockSize, "rw");

    }

    public MappedView(String path, int blockSize, String mode) {
        this.path = path;
        try {
            this.file = new RandomAccessFile(this.path, mode);
            this.fileChannel = file.getChannel();
            this.blockSize = blockSize;
            this.mode = mode;
            VIEW_POSITION_SHIFT = Utils.numberOfTwos(Math.max(4096, Utils.next2(this.blockSize)));
            VIEW_CHUNK_SIZE = (int) Math.pow(2, VIEW_POSITION_SHIFT);
            this.views = new MappedByteBuffer[1];

            long size = fileChannel.size();
            if(size > 0){
                final int viewIndex = (int)(size >>> VIEW_POSITION_SHIFT);
                views = new MappedByteBuffer[viewIndex + 1];
                int i =0;
                while (i<viewIndex + 1) {
                    views[i] = createBuffer( 1L  *i * VIEW_CHUNK_SIZE,
                          //(int) Math.min(VIEW_CHUNK_SIZE, size - (1L  *i * VIEW_CHUNK_SIZE))
                          VIEW_CHUNK_SIZE);
                    i++;
                }
            }
        } catch (IOException e) {
            throw new Error(e);
        }
    }

    @Override
    public int getInt(long offset) {
        byte b0 = getByte(offset + 0);
        byte b1 = getByte(offset + 1);
        byte b2 = getByte(offset + 2);
        byte b3 = getByte(offset + 3);

        return NATIVE == ByteOrder.BIG_ENDIAN ? makeInt(b0, b1, b2, b3) : makeInt(b3, b2, b1, b0);
    }

    @Override
    public long getLong(long offset) {
        byte b0 = getByte(offset + 0);
        byte b1 = getByte(offset + 1);
        byte b2 = getByte(offset + 2);
        byte b3 = getByte(offset + 3);
        byte b4 = getByte(offset + 4);
        byte b5 = getByte(offset + 5);
        byte b6 = getByte(offset + 6);
        byte b7 = getByte(offset + 7);

        return NATIVE == ByteOrder.BIG_ENDIAN ? makeLong(b0, b1, b2, b3, b4, b5, b6, b7) : makeLong(b7, b6, b5, b4, b3, b2, b1, b0);
    }

    @Override
    public void getBytes(long offset, byte[] value) {
        for (int i = 0; i < value.length; i++) {
            value[i] = getByte(offset + i);
        }
    }


    @Override
    public byte getByte(long offset) {
        acquire(offset);
        final int viewIndex = (int) (offset >>> VIEW_POSITION_SHIFT);
        long startPositionInViewChunk = offset - (1L * VIEW_CHUNK_SIZE * viewIndex);
        return views[viewIndex].get((int) startPositionInViewChunk) ;
    }

    @Override
    public void writeBytes(long offset, byte[] value) {
        for (int i = 0; i < value.length; i++) {
            writeByte(offset + i, value[i]);
        }
    }

    @Override
    public void writeByte(long offset, byte value) {
        acquire(offset);
        int viewIndex = (int) (offset >>> VIEW_POSITION_SHIFT);
        long startPositionInViewChunk = offset - (1L * VIEW_CHUNK_SIZE * viewIndex);
        try {

            views[viewIndex].put((int) startPositionInViewChunk, value);
        }
        catch (Exception e){
            System.out.println("");
        }
    }

    @Override
    public long size() {
        try {
            return fileChannel.size();
        } catch (IOException e) {
            throw new IOError(e);
        }
    }


    @Override
    public void flush() {
        flushBuffers();
    }

    @Override
    public void writeInt(long offset, int value) {
        writeByte(offset + 0, NATIVE == ByteOrder.BIG_ENDIAN ? int3(value) : int0(value));
        writeByte(offset + 1, NATIVE == ByteOrder.BIG_ENDIAN ? int2(value) : int1(value));
        writeByte(offset + 2, NATIVE == ByteOrder.BIG_ENDIAN ? int1(value) : int2(value));
        writeByte(offset + 3, NATIVE == ByteOrder.BIG_ENDIAN ? int0(value) : int3(value));
    }

    @Override
    public void writeLong(long offset, long value) {
        writeByte(offset + 0, NATIVE == ByteOrder.BIG_ENDIAN ? long7(value) : long0(value));
        writeByte(offset + 1, NATIVE == ByteOrder.BIG_ENDIAN ? long6(value) : long1(value));
        writeByte(offset + 2, NATIVE == ByteOrder.BIG_ENDIAN ? long5(value) : long2(value));
        writeByte(offset + 3, NATIVE == ByteOrder.BIG_ENDIAN ? long4(value) : long3(value));
        writeByte(offset + 4, NATIVE == ByteOrder.BIG_ENDIAN ? long3(value) : long4(value));
        writeByte(offset + 5, NATIVE == ByteOrder.BIG_ENDIAN ? long2(value) : long5(value));
        writeByte(offset + 6, NATIVE == ByteOrder.BIG_ENDIAN ? long1(value) : long6(value));
        writeByte(offset + 7, NATIVE == ByteOrder.BIG_ENDIAN ? long0(value) : long7(value));
    }

    private static byte int3(int x) {
        return (byte) (x >>> 24);
    }

    private static byte int2(int x) {
        return (byte) (x >>> 16);
    }

    private static byte int1(int x) {
        return (byte) (x >>> 8);
    }

    private static byte int0(int x) {
        return (byte) (x >>> 0);
    }

    private static byte long7(long x) {
        return (byte) (x >>> 56);
    }

    private static byte long6(long x) {
        return (byte) (x >>> 48);
    }

    private static byte long5(long x) {
        return (byte) (x >>> 40);
    }

    private static byte long4(long x) {
        return (byte) (x >>> 32);
    }

    private static byte long3(long x) {
        return (byte) (x >>> 24);
    }

    private static byte long2(long x) {
        return (byte) (x >>> 16);
    }

    private static byte long1(long x) {
        return (byte) (x >>> 8);
    }

    private static byte long0(long x) {
        return (byte) (x >>> 0);
    }

    private static int makeInt(byte b3, byte b2, byte b1, byte b0) {
        return (int) ((((b3 & 0xff) << 24) |
                ((b2 & 0xff) << 16) |
                ((b1 & 0xff) << 8) |
                ((b0 & 0xff) << 0)));
    }

    private static long makeLong(byte b7, byte b6, byte b5, byte b4,
                                 byte b3, byte b2, byte b1, byte b0) {
        return ((((long) b7 & 0xff) << 56) |
                (((long) b6 & 0xff) << 48) |
                (((long) b5 & 0xff) << 40) |
                (((long) b4 & 0xff) << 32) |
                (((long) b3 & 0xff) << 24) |
                (((long) b2 & 0xff) << 16) |
                (((long) b1 & 0xff) << 8) |
                (((long) b0 & 0xff) << 0));
    }

    protected ReferenceQueue<MappedByteBuffer> unreleasedQueue = new ReferenceQueue<MappedByteBuffer>();
    protected Set<Reference<MappedByteBuffer>> unreleasedChunks = new LinkedHashSet<Reference<MappedByteBuffer>>();

    //todo unmap is independent from filechannel close.reference queue
    @Override
    public void close() throws IOException {

//        fileChannel.force(true);
        fileChannel.close();
        file.close();

        flushBuffers();

        for (Reference<MappedByteBuffer> rb : unreleasedChunks) {
            MappedByteBuffer b = rb.get();
            if (b == null) continue;
            unmap(b);
        }

        //unmap & close
        for (int i = 0; i < views.length; i++) {
            if (views[i] != null) {
                unmap(views[i]);
            }
        }

        unreleasedChunks = null;
        unreleasedQueue = null;

        views = null;
    }

    public void flushBuffers() {
        //clear GC references
        for (Reference ref = unreleasedQueue.poll(); ref != null; ref = unreleasedQueue.poll()) {
            unreleasedChunks.remove(ref);
        }

        for (Reference<MappedByteBuffer> rb : unreleasedChunks) {
            MappedByteBuffer b = rb.get();
            if (b == null) continue;
            b.force();
        }
        for (ByteBuffer b : views) {
            if (b != null && (b instanceof MappedByteBuffer)) {
                ((MappedByteBuffer) b).force();
            }
        }
    }

    //todo throw IOError
    public void acquire(long offset) {
        if (!checkAvailable(offset)) {
            throw new Error("exceeded limit");
        }
    }


    //todo if size limit exceeds return false
    private boolean checkAvailable(long offset) {

        return tryAvailable(offset);

    }

    public final boolean tryAvailable(long offset) {
        final int viewIndex = (int) (offset >>> VIEW_POSITION_SHIFT);

        try {
            if (viewIndex >= views.length) {
                views = Arrays.copyOf(views, Math.max(viewIndex + 1, views.length * 2));
            }

            if (views[viewIndex] == null) {
                views[viewIndex] = createBuffer(offset, VIEW_CHUNK_SIZE);
            }

        } catch (Exception e) {
            System.out.println("offset " + offset + " pos " + viewIndex + " views length " + views.length);
            throw new Error(e);
        }


        return true;
    }


    public MappedByteBuffer testBuffer(long offset, int size) {
        return createBuffer(offset,size);
    }

    private MappedByteBuffer createBuffer(long offset, int size) {
        MappedByteBuffer bucket = null;
        try {
            bucket = fileChannel.map(this.mode.equals("rw") ? FileChannel.MapMode.READ_WRITE : FileChannel.MapMode.READ_ONLY,
                    offset,
                    size);
            bucket.order(ByteOrder.nativeOrder());

//            bucket = MapUtils.getMap(fileChannel,offset,size);
            unreleasedChunks.add(new WeakReference<MappedByteBuffer>(bucket, unreleasedQueue));

            for (Reference ref = unreleasedQueue.poll(); ref != null; ref = unreleasedQueue.poll()) {
                unreleasedChunks.remove(ref);
            }

        } catch (Exception e) {
            System.out.println("offset-->" + offset + "\tsize -->" + size);
            new Error(e);
        }

        return bucket;
    }


    /**
     * Hack to unmap MappedByteBuffer.
     * Unmap is necessary on Windows, otherwise file is locked until JVM exits or BB is GCed.
     * There is no public JVM API to unmap buffer, so this tries to use SUN proprietary API for unmap.
     * Any error is silently ignored (for example SUN API does not exist on Android).
     */
    protected void unmap(MappedByteBuffer b) {
        try {
            if (unmapHackSupported) {

                // need to dispose old direct buffer, see bug
                // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4724038
                Method cleanerMethod = b.getClass().getMethod("cleaner", new Class[0]);
                if (cleanerMethod != null) {
                    cleanerMethod.setAccessible(true);
                    Object cleaner = cleanerMethod.invoke(b, new Object[0]);
                    if (cleaner != null) {
                        Method clearMethod = cleaner.getClass().getMethod("clean", new Class[0]);
                        if (cleanerMethod != null)
                            clearMethod.invoke(cleaner, new Object[0]);
                    }
                }
            }
        } catch (Exception e) {
            unmapHackSupported = false;
        }
    }

    private static boolean unmapHackSupported = true;

    static {
        try {
            unmapHackSupported =
                    Class.forName("sun.nio.ch.DirectBuffer") != null;
        } catch (Exception e) {
            unmapHackSupported = false;
        }
    }


}