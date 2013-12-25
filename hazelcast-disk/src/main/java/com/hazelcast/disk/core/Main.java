package com.hazelcast.disk.core;

import com.hazelcast.config.GlobalSerializerConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.serialization.SerializationServiceBuilder;
import com.hazelcast.nio.serialization.StreamSerializer;

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


    public static void main(String[] args) throws IOException {


        testWR();
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
        directory.loadLastIndexFilePosition();

        directory.close();

    }


}
