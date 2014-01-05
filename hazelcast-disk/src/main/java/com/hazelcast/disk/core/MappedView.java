package com.hazelcast.disk.core;

import com.hazelcast.disk.Storage;

import java.io.IOError;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

/**
 * @author: ahmetmircik
 * Date: 1/1/14
 */
public class MappedView implements Storage {

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
        } catch (IOException e) {
            throw new Error(e);
        }
    }

    @Override
    public int getInt(long offset) {
        acquire(offset);
        int viewIndex = (int) offset / VIEW_CHUNK_SIZE;
        long startPositionInViewChunk = offset - (VIEW_CHUNK_SIZE * viewIndex);
        try {
            byte b0 = views[viewIndex].get((int) startPositionInViewChunk++);
            if (startPositionInViewChunk + 1 > views[viewIndex].capacity()) {
                viewIndex++;
                startPositionInViewChunk = 0;
                acquire(viewIndex * VIEW_CHUNK_SIZE);
            }
            byte b1 = views[viewIndex].get((int) startPositionInViewChunk++);
            if (startPositionInViewChunk + 1 > views[viewIndex].capacity()) {
                viewIndex++;
                startPositionInViewChunk = 0;
                acquire(viewIndex * VIEW_CHUNK_SIZE);
            }
            byte b2 = views[viewIndex].get((int) startPositionInViewChunk++);
            if (startPositionInViewChunk + 1 > views[viewIndex].capacity()) {
                viewIndex++;
                startPositionInViewChunk = 0;
                acquire(viewIndex * VIEW_CHUNK_SIZE);
            }
            byte b3 = views[viewIndex].get((int) startPositionInViewChunk++);
            return NATIVE == ByteOrder.BIG_ENDIAN ? makeInt(b0,b1,b2,b3) : makeInt(b3,b2,b1,b0);

        }
        catch(Throwable t)
        {
            System.out.println("offset\t" + offset + " startPositionInViewChunk\t" + startPositionInViewChunk + " view index\t" + viewIndex);
            throw new Error(t);
        }
    }

    @Override
    public void getBytes(long offset, byte[] value) {
        acquire(offset);
        int viewIndex = (int) offset / VIEW_CHUNK_SIZE;
        long startPositionInViewChunk = offset - (VIEW_CHUNK_SIZE * viewIndex);
        try {
            for (int i = 0; i < value.length; i++) {
                value[i] = views[viewIndex].get((int) startPositionInViewChunk++);
                if (startPositionInViewChunk + 1 > views[viewIndex].capacity()) {
                    viewIndex++;
                    startPositionInViewChunk = 0;
                    acquire(viewIndex * VIEW_CHUNK_SIZE);
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
        int viewIndex = (int) offset / VIEW_CHUNK_SIZE;
        long startPositionInViewChunk = offset - (VIEW_CHUNK_SIZE * viewIndex);
        try {
            for (int i = 0; i < value.length; i++) {
                views[viewIndex].put((int) startPositionInViewChunk++, value[i]);
                if (startPositionInViewChunk + 1 > views[viewIndex].capacity()) {
                    viewIndex++;
                    startPositionInViewChunk = 0;
                    acquire(viewIndex * VIEW_CHUNK_SIZE);
                }
            }
        } catch (Throwable t) {
            System.out.println("offset\t" + offset + " startPositionInViewChunk\t" + startPositionInViewChunk + " view index\t" + viewIndex);
            throw new Error(t);
        }
    }

    ByteOrder NATIVE = ByteOrder.nativeOrder();
    @Override
    public void writeInt(long offset, int value) {
        acquire(offset);
        int viewIndex = (int) offset / VIEW_CHUNK_SIZE;
        long startPositionInViewChunk = offset - (VIEW_CHUNK_SIZE * viewIndex);
        try {
            views[viewIndex].put((int) startPositionInViewChunk++, NATIVE == ByteOrder.BIG_ENDIAN ? int3(value) : int0(value));
            if (startPositionInViewChunk + 1 > views[viewIndex].capacity()) {
                viewIndex++;
                startPositionInViewChunk = 0;
                acquire(viewIndex * VIEW_CHUNK_SIZE);
            }
            views[viewIndex].put((int) startPositionInViewChunk++, NATIVE == ByteOrder.BIG_ENDIAN ? int2(value) : int1(value));
            if (startPositionInViewChunk + 1 > views[viewIndex].capacity()) {
                viewIndex++;
                startPositionInViewChunk = 0;
                acquire(viewIndex * VIEW_CHUNK_SIZE);
            }
            views[viewIndex].put((int) startPositionInViewChunk++, NATIVE == ByteOrder.BIG_ENDIAN ? int1(value) : int2(value));
            if (startPositionInViewChunk + 1 > views[viewIndex].capacity()) {
                viewIndex++;
                startPositionInViewChunk = 0;
                acquire(viewIndex * VIEW_CHUNK_SIZE);
            }
            views[viewIndex].put((int) startPositionInViewChunk++, NATIVE == ByteOrder.BIG_ENDIAN ? int0(value) : int3(value));
        }
        catch(Throwable t)
        {
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

    private static int makeInt(byte b3, byte b2, byte b1, byte b0) {
        return (int)((((b3 & 0xff) << 24) |
                ((b2 & 0xff) << 16) |
                ((b1 & 0xff) <<  8) |
                ((b0 & 0xff) <<  0)));
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

public static final int CHUNK_SHIFT = 30;

public static final int CHUNK_SIZE = 1 << CHUNK_SHIFT;

public static final int CHUNK_SIZE_MOD_MASK = CHUNK_SIZE - 1;

public static final int VIEW_CHUNK_SIZE = 1 << 30;

    public final boolean tryAvailable(long offset) {
        final int pos = (int) offset / VIEW_CHUNK_SIZE;

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

//    public final boolean tryAvailable(long offset) {
//
//        int chunkPos = (int) (offset >>> CHUNK_SHIFT);
//
//        //check for most common case, this is already mapped
//        if (chunkPos < views.length && views[chunkPos] != null &&
//                views[chunkPos].capacity() >= (offset & CHUNK_SIZE_MOD_MASK)) {
//            return true;
//        }
//
//        try {
//            //check second time
//            if (chunkPos < views.length && views[chunkPos] != null &&
//                    views[chunkPos].capacity() >= (offset & CHUNK_SIZE_MOD_MASK))
//                return true;
//
//            ByteBuffer[] chunks2 = views;
//
//            //grow array if necessary
//            if (chunkPos >= chunks2.length) {
//                chunks2 = Arrays.copyOf(chunks2, Math.max(chunkPos + 1, chunks2.length * 2));
//            }
//
//
//            //just remap file buffer
//            if (chunks2[chunkPos] == null) {
//                //make sure previous buffer is fully expanded
//                if (chunkPos > 0) {
//                    ByteBuffer oldPrev = chunks2[chunkPos - 1];
//                    if (oldPrev == null || oldPrev.capacity() != CHUNK_SIZE) {
//                        chunks2[chunkPos - 1] = makeNewBuffer(1L * chunkPos * CHUNK_SIZE - 1, chunks2);
//                    }
//                }
//            }
//
//
//            ByteBuffer newChunk = makeNewBuffer(offset, chunks2);
//
//            chunks2[chunkPos] = newChunk;
//
//            views = chunks2;
//        } finally {
//        }
//        return true;
//    }

static final int BUF_SIZE_INC = 1024 * 1024;


    protected ByteBuffer makeNewBuffer(long offset, ByteBuffer[] buffers2) {
        try {
            //create new chunk
            long newChunkSize = offset & CHUNK_SIZE_MOD_MASK;
            int round = offset < BUF_SIZE_INC ? BUF_SIZE_INC / 16 : BUF_SIZE_INC;

            //round newBufSize to multiple of BUF_SIZE_INC
            long rest = newChunkSize % round;
            if (rest != 0)
                newChunkSize += round - rest;

            /**
             *
             *  newChunkSize must be <= Integer.MAX_VALUE
             *
             * */
            final MappedByteBuffer buf = fileChannel.map(FileChannel.MapMode.READ_WRITE,
                    offset - (offset & CHUNK_SIZE_MOD_MASK), newChunkSize);


            return buf;
        } catch (IOException e) {
            if (e.getCause() != null && e.getCause() instanceof OutOfMemoryError) {
                throw new RuntimeException("File could not be mapped to memory, common problem on 32bit JVM. Use `DBMaker.newRandomAccessFileDB()` as workaround", e);
            }

            throw new IOError(e);
        }
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

static{
        try{
        unmapHackSupported=
        Class.forName("sun.nio.ch.DirectBuffer")!=null;
        }catch(Exception e){
        unmapHackSupported=false;
        }
        }


        }