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

/**
 * @author: ahmetmircik
 * Date: 12/23/13
 */
public class Main {

    private  static SerializationConfig serializationConfig = new SerializationConfig().setGlobalSerializerConfig(
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


    static void test(){
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

    public static void main(String[] args) throws IOException {

        System.out.println((- 1 & 0X7FFFFFFF) );

        System.out.println(1L + Integer.MAX_VALUE);
        System.out.println( ~(-41));
        //test();
        final long start = System.nanoTime();
        System.out.println(1<<32);
        final long diff = System.nanoTime() - start;
        System.out.println("diff1 " + diff);

        final long start2 = System.nanoTime();
        System.out.println((int)Math.pow(2,32));

        final long diff2 = System.nanoTime() - start2;
        System.out.println("diff2 " + diff2);
//          Storages.createMMap("",0,0);


//        testWR();
//        Directory directory = new Directory("1vhazel");
//        directory.createFiles();
//
//        DummyValue value = new DummyValue("test", 111);
//        Data data = ss1.toData(value);
//
//        directory.insert(data,data);

    }

    static void testWR() throws IOException {
        final Directory directory = new Directory("testWR");
        directory.createFiles();

       // directory.testWrite();
//        directory.loadLastIndexFilePosition();

        directory.close();

    }


}
