package com.hazelcast.disk.core;

import com.hazelcast.nio.serialization.Data;
import sun.nio.ch.DirectBuffer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author: ahmetmircik
 * Date: 12/27/13
 */
public class ReadDataFile {

    private String path;

    private RandomAccessFile file;
    private FileChannel fileChannel;

    public ReadDataFile(String path) {
        this.path = path;
        createFiles();
    }

    private void createFiles() {
        try {
            this.file = new RandomAccessFile(path + ".data", "rw");
            this.fileChannel = file.getChannel();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private MappedByteBuffer getView(int offset, long size) {
        MappedByteBuffer bucket = null;
        try {
            bucket = fileChannel.map(FileChannel.MapMode.READ_WRITE,
                    offset, size);
            bucket.order(ByteOrder.nativeOrder());
        } catch (IOException e) {
            e.printStackTrace();
        }

        return bucket;
    }

    //depth of bucket
    static final int BUCKET_LENGTH = Integer.SIZE +
            Integer.SIZE +
            Bucket.NUMBER_OF_RECORDS * Bucket.SIZE_OF_RECORD;

    int total = 0;

    public void print() throws IOException {

        final long fileLength = file.length();
        final long num = fileLength / BUCKET_LENGTH;
        for (int i = 0; i < num; i++) {
            final MappedByteBuffer view = getView(BUCKET_LENGTH * i, BUCKET_LENGTH);
            getRecord(view);
        }

        System.out.println("total\t" + total);

    }

    public void printAddress() throws IOException {

        final MappedByteBuffer view = getView(0, BUCKET_LENGTH);
        System.out.println("total\t" + ((DirectBuffer)view).address());

    }


    private void getRecord(MappedByteBuffer bucket) {
        final int bucketDepth = bucket.getInt();
        final int numberOfElements = bucket.getInt();
        total += numberOfElements;
        for (int i = 0; i < numberOfElements; i++) {
            final int keyLen = bucket.getInt();
            byte[] bytes = new byte[keyLen];
            bucket.get(bytes);
            Data keyGot = new Data(0, bytes);
            final int recordLen = bucket.getInt();
            byte[] bytes2 = new byte[recordLen];
            bucket.get(bytes2);
            Data recordGot = new Data(0, bytes2);
//            System.out.println("keyGot "+keyGot.getBuffer().length);
//            System.out.println("recordGot "+recordGot.getBuffer().length);
        }
    }


    public void close() {
        try {
            fileChannel.close();
            file.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        final ReadDataFile readDataFile = new ReadDataFile("ooo1");
        readDataFile.printAddress();
    }

}
