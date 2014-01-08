package com.hazelcast.disk.core;

import com.hazelcast.config.GlobalSerializerConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.serialization.SerializationServiceBuilder;
import com.hazelcast.nio.serialization.StreamSerializer;

import javax.swing.filechooser.FileSystemView;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;

/**
 * @author: ahmetmircik
 * Date: 12/23/13
 */
public class Main {

    private static SerializationConfig serializationConfig = new SerializationConfig().setGlobalSerializerConfig(
            new GlobalSerializerConfig().setImplementation(new StreamSerializer<DummyValue>() {
                public void write(ObjectDataOutput out, DummyValue v) throws IOException {
                    out.writeUTF(v.s);
                    out.writeInt(v.k);
                }

                public DummyValue read(ObjectDataInput in) throws IOException {
                    return new DummyValue(in.readUTF(), in.readInt());
                }

                public int getTypeId() {
                    return 123;
                }

                public void destroy() {
                }
            }));

    public static final SerializationService ss1 = new SerializationServiceBuilder().setConfig(serializationConfig).build();

    private static class DummyValue {
        String s;
        int k;

        private DummyValue(String s, int k) {
            this.s = s;
            this.k = k;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            DummyValue that = (DummyValue) o;

            if (k != that.k) return false;
            if (s != null ? !s.equals(that.s) : that.s != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = s != null ? s.hashCode() : 0;
            result = 31 * result + k;
            return result;
        }
    }


    static void test() {
        System.out.println("File system roots returned byFileSystemView.getFileSystemView():");
        FileSystemView fsv = FileSystemView.getFileSystemView();
        File[] roots = fsv.getRoots();
        for (int i = 0; i < roots.length; i++) {
            System.out.println("Root: " + roots[i]);
        }

        System.out.println("Home directory: " + fsv.getHomeDirectory());

        System.out.println("File system roots returned by File.listRoots():");
        File[] f = File.listRoots();
        for (int i = 0; i < f.length; i++) {
            System.out.println("Drive: " + f[i]);
            System.out.println("Display name: " + fsv.getSystemDisplayName(f[i]));
            System.out.println("Is drive: " + fsv.isDrive(f[i]));
            System.out.println("Is floppy: " + fsv.isFloppyDrive(f[i]));
            System.out.println("Readable: " + f[i].canRead());
            System.out.println("Writable: " + f[i].canWrite());
            System.out.println("Total space: " + f[i].getTotalSpace());
            System.out.println("Usable space: " + f[i].getUsableSpace());
        }
    }

    static int globalDepth = 7;

    private static Integer[] newRange0(int index, final int bucketDepth) {
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
        final Integer[] addressList = new Integer[strings.size()];
        int i = 0;
        while ((tmp = strings.poll()) != null) {
            addressList[i++] = Integer.valueOf(tmp, 2);
        }
        return addressList;
    }

    private static String reverseFirstBits(String s) {
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
    private static String toBinary(int hash, int depth) {
        String s = Integer.toBinaryString(hash);
        while (s.length() < depth) {
            s = "0" + s;
        }
        return s;
    }


    private static int[] newRange1(int index, int bucketDepth) {
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
            for (int i = 0; i < x.length; i += 2) {
                if (x[i] == -1) continue;
                final Integer param = x[i];
                x[i] = -1;
                x[i] = (param | mask);
                x[i + 1] = (param & ~mask);
            }
            bucketDepth++;
        }
        Arrays.sort(x);
        return x;
    }


    public static void main(String[] args) throws IOException {

        long diff = 0;
        for (int i = 0; i < 10000; i++) {
            final long l = System.nanoTime();
            newRange0(i, 1);
            diff += System.nanoTime() - l;
        }

        long diff2 = 0;
        for (int i = 0; i < 10000; i++) {
            final long l = System.nanoTime();
            newRange1(i, 1);
            diff2 += System.nanoTime() - l;
        }

        System.out.println(diff / 10000 + "....." + diff2 / 10000);

    }

    static void testWR() throws IOException {
        final Directory directory = new Directory("testWR");
        directory.createFiles();

        // directory.testWrite();
//        directory.loadLastIndexFilePosition();

        directory.close();

    }


}
