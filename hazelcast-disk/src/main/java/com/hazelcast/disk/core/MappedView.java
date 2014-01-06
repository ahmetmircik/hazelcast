package com.hazelcast.disk.core;

import com.hazelcast.disk.Storage;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

/**
 * @author: ahmetmircik
 * Date: 1/1/14
 */
public class MappedView implements Storage {

    private ByteOrder NATIVE = ByteOrder.nativeOrder();
    public  final int VIEW_CHUNK_SIZE ;
    private final String path;
    private final RandomAccessFile file;
    private final FileChannel fileChannel;

    private MappedByteBuffer[] views;

    private int blockSize;


    public MappedView(String path, int blockSize) {
        this.path = path;
        try {
            this.file = new RandomAccessFile(this.path, "rw");
            this.fileChannel = file.getChannel();
            this.views = new MappedByteBuffer[1];
            this.blockSize = blockSize;
            VIEW_CHUNK_SIZE = this.blockSize;
        } catch (IOException e) {
            throw new Error(e);
        }
    }

    @Override
    public int getInt(long offset) {
        acquire(offset);
        int viewIndex = (int) (offset / VIEW_CHUNK_SIZE);
        long startPositionInViewChunk = offset - (1L * VIEW_CHUNK_SIZE * viewIndex);
        try {
            byte b0 = views[viewIndex].get((int) startPositionInViewChunk++);
            if (startPositionInViewChunk + 1 > views[viewIndex].capacity()) {
                viewIndex++;
                startPositionInViewChunk = 0;
                acquire(1L * viewIndex * VIEW_CHUNK_SIZE);
            }
            byte b1 = views[viewIndex].get((int) startPositionInViewChunk++);
            if (startPositionInViewChunk + 1 > views[viewIndex].capacity()) {
                viewIndex++;
                startPositionInViewChunk = 0;
                acquire(1L * viewIndex * VIEW_CHUNK_SIZE);
            }
            byte b2 = views[viewIndex].get((int) startPositionInViewChunk++);
            if (startPositionInViewChunk + 1 > views[viewIndex].capacity()) {
                viewIndex++;
                startPositionInViewChunk = 0;
                acquire(1L * viewIndex * VIEW_CHUNK_SIZE);
            }
            byte b3 = views[viewIndex].get((int) startPositionInViewChunk++);
            return NATIVE == ByteOrder.BIG_ENDIAN ? makeInt(b0, b1, b2, b3) : makeInt(b3, b2, b1, b0);

        } catch (Throwable t) {
            System.out.println("offset\t" + offset + " startPositionInViewChunk\t" + startPositionInViewChunk + " view index\t" + viewIndex);
            throw new Error(t);
        }
    }

    @Override
    public long getLong(long offset) {
        acquire(offset);
        int viewIndex = (int) (offset / VIEW_CHUNK_SIZE);
        long startPositionInViewChunk = offset - (1L * VIEW_CHUNK_SIZE * viewIndex);
        try {
            byte b0 = views[viewIndex].get((int) startPositionInViewChunk++);
            if (startPositionInViewChunk + 1 > views[viewIndex].capacity()) {
                viewIndex++;
                startPositionInViewChunk = 0;
                acquire(1L * viewIndex * VIEW_CHUNK_SIZE);
            }
            byte b1 = views[viewIndex].get((int) startPositionInViewChunk++);
            if (startPositionInViewChunk + 1 > views[viewIndex].capacity()) {
                viewIndex++;
                startPositionInViewChunk = 0;
                acquire(1L * viewIndex * VIEW_CHUNK_SIZE);
            }
            byte b2 = views[viewIndex].get((int) startPositionInViewChunk++);
            if (startPositionInViewChunk + 1 > views[viewIndex].capacity()) {
                viewIndex++;
                startPositionInViewChunk = 0;
                acquire(1L * viewIndex * VIEW_CHUNK_SIZE);
            }
            byte b3 = views[viewIndex].get((int) startPositionInViewChunk++);
            if (startPositionInViewChunk + 1 > views[viewIndex].capacity()) {
                viewIndex++;
                startPositionInViewChunk = 0;
                acquire(1L * viewIndex * VIEW_CHUNK_SIZE);
            }
            byte b4 = views[viewIndex].get((int) startPositionInViewChunk++);
            if (startPositionInViewChunk + 1 > views[viewIndex].capacity()) {
                viewIndex++;
                startPositionInViewChunk = 0;
                acquire(1L * viewIndex * VIEW_CHUNK_SIZE);
            }
            byte b5 = views[viewIndex].get((int) startPositionInViewChunk++);
            if (startPositionInViewChunk + 1 > views[viewIndex].capacity()) {
                viewIndex++;
                startPositionInViewChunk = 0;
                acquire(1L * viewIndex * VIEW_CHUNK_SIZE);
            }
            byte b6 = views[viewIndex].get((int) startPositionInViewChunk++);
            if (startPositionInViewChunk + 1 > views[viewIndex].capacity()) {
                viewIndex++;
                startPositionInViewChunk = 0;
                acquire(1L * viewIndex * VIEW_CHUNK_SIZE);
            }
            byte b7 = views[viewIndex].get((int) startPositionInViewChunk++);
            if (startPositionInViewChunk + 1 > views[viewIndex].capacity()) {
                viewIndex++;
                startPositionInViewChunk = 0;
                acquire(1L * viewIndex * VIEW_CHUNK_SIZE);
            }
            return NATIVE == ByteOrder.BIG_ENDIAN ? makeLong(b0, b1, b2, b3, b4, b5, b6, b7) : makeLong(b7, b6, b5, b4, b3, b2, b1, b0);

        } catch (Throwable t) {
            System.out.println("offset\t" + offset + " startPositionInViewChunk\t" + startPositionInViewChunk + " view index\t" + viewIndex);
            throw new Error(t);
        }
    }

    @Override
    public void getBytes(long offset, byte[] value) {
        acquire(offset);
        int viewIndex = (int) (offset / VIEW_CHUNK_SIZE);
        long startPositionInViewChunk = offset - (1L * VIEW_CHUNK_SIZE * viewIndex);
        try {
            for (int i = 0; i < value.length; i++) {
                value[i] = views[viewIndex].get((int) startPositionInViewChunk++);
                if (startPositionInViewChunk + 1 > views[viewIndex].capacity()) {
                    viewIndex++;
                    startPositionInViewChunk = 0;
                    acquire(1L * viewIndex * VIEW_CHUNK_SIZE);
                }
            }
        } catch (Throwable t) {
            System.out.println("offset\t" + offset + " startPositionInViewChunk\t" + startPositionInViewChunk + " view index\t" + viewIndex);
            throw new Error(t);
        }
    }

    @Override
    public void writeBytes(long offset, byte[] value) {
        acquire(offset);
        int viewIndex = (int) (offset / VIEW_CHUNK_SIZE);
        long startPositionInViewChunk = offset - (1L * VIEW_CHUNK_SIZE * viewIndex);
        try {
            for (int i = 0; i < value.length; i++) {
                views[viewIndex].put((int) startPositionInViewChunk++, value[i]);
                if (startPositionInViewChunk + 1 > views[viewIndex].capacity()) {
                    viewIndex++;
                    startPositionInViewChunk = 0;
                    acquire(1L * viewIndex * VIEW_CHUNK_SIZE);
                }
            }
        } catch (Throwable t) {
            System.out.println("offset\t" + offset + " startPositionInViewChunk\t" + startPositionInViewChunk + " view index\t" + viewIndex);
            throw new Error(t);
        }
    }


    @Override
    public void writeInt(long offset, int value) {
        acquire(offset);
        int viewIndex = (int) (offset / VIEW_CHUNK_SIZE);
        long startPositionInViewChunk = offset - (1L * VIEW_CHUNK_SIZE * viewIndex);
        try {
            views[viewIndex].put((int) startPositionInViewChunk++, NATIVE == ByteOrder.BIG_ENDIAN ? int3(value) : int0(value));
            if (startPositionInViewChunk + 1 > views[viewIndex].capacity()) {
                viewIndex++;
                startPositionInViewChunk = 0;
                acquire(1L * viewIndex * VIEW_CHUNK_SIZE);
            }
            views[viewIndex].put((int) startPositionInViewChunk++, NATIVE == ByteOrder.BIG_ENDIAN ? int2(value) : int1(value));
            if (startPositionInViewChunk + 1 > views[viewIndex].capacity()) {
                viewIndex++;
                startPositionInViewChunk = 0;
                acquire(1L * viewIndex * VIEW_CHUNK_SIZE);
            }
            views[viewIndex].put((int) startPositionInViewChunk++, NATIVE == ByteOrder.BIG_ENDIAN ? int1(value) : int2(value));
            if (startPositionInViewChunk + 1 > views[viewIndex].capacity()) {
                viewIndex++;
                startPositionInViewChunk = 0;
                acquire(1L * viewIndex * VIEW_CHUNK_SIZE);
            }
            views[viewIndex].put((int) startPositionInViewChunk++, NATIVE == ByteOrder.BIG_ENDIAN ? int0(value) : int3(value));
        } catch (Throwable t) {
            System.out.println("offset\t" + offset + " startPositionInViewChunk\t" + startPositionInViewChunk + " view index\t" + viewIndex);
            throw new Error(t);
        }
    }

    @Override
    public void writeLong(long offset, long value) {
        acquire(offset);
        int viewIndex = (int) (offset / VIEW_CHUNK_SIZE);
        long startPositionInViewChunk = offset - (1L * VIEW_CHUNK_SIZE * viewIndex);
        try {
            views[viewIndex].put((int) startPositionInViewChunk++, NATIVE == ByteOrder.BIG_ENDIAN ? long7(value) : long0(value));
            if (startPositionInViewChunk + 1 > views[viewIndex].capacity()) {
                viewIndex++;
                startPositionInViewChunk = 0;
                acquire(1L * viewIndex * VIEW_CHUNK_SIZE);
            }
            views[viewIndex].put((int) startPositionInViewChunk++, NATIVE == ByteOrder.BIG_ENDIAN ? long6(value) : long1(value));
            if (startPositionInViewChunk + 1 > views[viewIndex].capacity()) {
                viewIndex++;
                startPositionInViewChunk = 0;
                acquire(1L * viewIndex * VIEW_CHUNK_SIZE);
            }
            views[viewIndex].put((int) startPositionInViewChunk++, NATIVE == ByteOrder.BIG_ENDIAN ? long5(value) : long2(value));
            if (startPositionInViewChunk + 1 > views[viewIndex].capacity()) {
                viewIndex++;
                startPositionInViewChunk = 0;
                acquire(1L * viewIndex * VIEW_CHUNK_SIZE);
            }
            views[viewIndex].put((int) startPositionInViewChunk++, NATIVE == ByteOrder.BIG_ENDIAN ? long4(value) : long3(value));
            if (startPositionInViewChunk + 1 > views[viewIndex].capacity()) {
                viewIndex++;
                startPositionInViewChunk = 0;
                acquire(1L * viewIndex * VIEW_CHUNK_SIZE);
            }
            views[viewIndex].put((int) startPositionInViewChunk++, NATIVE == ByteOrder.BIG_ENDIAN ? long3(value) : long4(value));
            if (startPositionInViewChunk + 1 > views[viewIndex].capacity()) {
                viewIndex++;
                startPositionInViewChunk = 0;
                acquire(1L * viewIndex * VIEW_CHUNK_SIZE);
            }
            views[viewIndex].put((int) startPositionInViewChunk++, NATIVE == ByteOrder.BIG_ENDIAN ? long2(value) : long5(value));
            if (startPositionInViewChunk + 1 > views[viewIndex].capacity()) {
                viewIndex++;
                startPositionInViewChunk = 0;
                acquire(1L * viewIndex * VIEW_CHUNK_SIZE);
            }
            views[viewIndex].put((int) startPositionInViewChunk++, NATIVE == ByteOrder.BIG_ENDIAN ? long1(value) : long6(value));
            if (startPositionInViewChunk + 1 > views[viewIndex].capacity()) {
                viewIndex++;
                startPositionInViewChunk = 0;
                acquire(1L * viewIndex * VIEW_CHUNK_SIZE);
            }
            views[viewIndex].put((int) startPositionInViewChunk++, NATIVE == ByteOrder.BIG_ENDIAN ? long0(value) : long7(value));
            if (startPositionInViewChunk + 1 > views[viewIndex].capacity()) {
                viewIndex++;
                startPositionInViewChunk = 0;
                acquire(1L * viewIndex * VIEW_CHUNK_SIZE);
            }
        } catch (Throwable t) {
            System.out.println("offset\t" + offset + " startPositionInViewChunk\t" + startPositionInViewChunk + " view index\t" + viewIndex);
            throw new Error(t);
        }
    }

    private static byte int3(int x) {
        return (byte) (x >> 24);
    }

    private static byte int2(int x) {
        return (byte) (x >> 16);
    }

    private static byte int1(int x) {
        return (byte) (x >> 8);
    }

    private static byte int0(int x) {
        return (byte) (x >> 0);
    }

    private static byte long7(long x) {
        return (byte) (x >> 56);
    }

    private static byte long6(long x) {
        return (byte) (x >> 48);
    }

    private static byte long5(long x) {
        return (byte) (x >> 40);
    }

    private static byte long4(long x) {
        return (byte) (x >> 32);
    }

    private static byte long3(long x) {
        return (byte) (x >> 24);
    }

    private static byte long2(long x) {
        return (byte) (x >> 16);
    }

    private static byte long1(long x) {
        return (byte) (x >> 8);
    }

    private static byte long0(long x) {
        return (byte) (x >> 0);
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

    //todo unmap is independent from filechannel close.reference queue
    @Override
    public void close() throws IOException {
        fileChannel.close();
        file.close();
        //unmap & close
        for (int i = 0; i < views.length; i++) {
            if (views[i] != null) {
                unmap((MappedByteBuffer) views[i]);
            }
        }
    }

    //todo throw IOError
    public void acquire(long offset) {
        if (!checkAvailable(offset)) {
            throw new Error("exceeded limit");
        }
    }


    //todo offset should be long ???
    //todo if size limit exceeds return false
    private boolean checkAvailable(long offset) {

        return tryAvailable(offset);

    }

    public final boolean tryAvailable(long offset) {
        final int pos = (int) (offset / VIEW_CHUNK_SIZE);

        try {
            if (pos >= views.length) {
                views = Arrays.copyOf(views, Math.max(pos + 1, views.length * 2));
            }

            if (views[pos] == null) {
                views[pos] = createBuffer(offset, VIEW_CHUNK_SIZE);
            }

        } catch (Exception e) {
            System.out.println("offset " + offset + " pos " + pos + " views length " + views.length);
            throw new Error(e);
        }


        return true;
    }

    private MappedByteBuffer createBuffer(long offset, int size) {
        MappedByteBuffer bucket = null;
        try {
            bucket = fileChannel.map(FileChannel.MapMode.READ_WRITE,
                    offset,
                    size);
            bucket.order(ByteOrder.nativeOrder());
        } catch (Exception e) {
            System.out.println("negative position-->" + offset + "\tsize -->" + size);
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