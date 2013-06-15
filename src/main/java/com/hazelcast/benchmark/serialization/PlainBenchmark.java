/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.benchmark.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.UnsafeInput;
import com.esotericsoftware.kryo.io.UnsafeOutput;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.serialization.*;

import java.io.*;

public class PlainBenchmark {

    static final int COUNT = 10000;
    static final int BUFFER_SIZE = 64 * 1024;

    public static void main(String[] args) throws Exception {
        for (int i = 0; i < 3; i++) {
            testJDK();
            testDS();
            testIDS();
            testKryo();
            testKryoUnsafe();

            System.out.println();
            System.out.println();
        }
    }

    private static void gc() throws InterruptedException {
        System.gc();
        Thread.sleep(1000);
    }

    private static void testJDK() throws Exception {
        final Factory factory = new Factory() {
            SampleObject create(int i) {
                return new SampleObject(i);
            }
        };

        final Serializer serializer = new Serializer() {
            public byte[] write(Object o) throws IOException {
                ByteArrayOutputStream bout = new ByteArrayOutputStream(BUFFER_SIZE);
                ObjectOutputStream out = new ObjectOutputStream(bout);
                out.writeObject(o);
                return bout.toByteArray();
            }

            public Object read(byte[] data) throws IOException, ClassNotFoundException {
                ByteArrayInputStream bin = new ByteArrayInputStream(data);
                ObjectInputStream in = new ObjectInputStream(bin);
                return in.readObject();
            }
        };

        test(serializer, "Java Serialization", COUNT, factory);
    }

    private static void testDS() throws Exception {
        final Serializer serializer = new Serializer() {
            final SerializationService ss = new SerializationServiceBuilder().build();

            public byte[] write(Object o) throws IOException {
                BufferObjectDataOutput out = ss.createObjectDataOutput(BUFFER_SIZE);
                out.writeUTF(o.getClass().getName());
                ((DataSerializable) o).writeData(out);
                return out.toByteArray();
            }

            public Object read(byte[] data) throws IOException, ClassNotFoundException {
                BufferObjectDataInput in = ss.createObjectDataInput(data);
                String className = in.readUTF();
                try {
                    DataSerializable ds = ClassLoaderUtil.newInstance(in.getClassLoader(), className);
                    ds.readData(in);
                    return ds;
                } catch (Exception e) {
                    throw new ClassNotFoundException(className, e);
                }
            }
        };

        final Factory factory = new Factory() {
            SampleObject create(int i) {
                return new DSSampleObject(i);
            }
        };

        test(serializer, "DataSerializable", COUNT, factory);
    }

    private static void testIDS() throws Exception {
        final Serializer serializer = new Serializer() {
            final SerializationService ss = new SerializationServiceBuilder().build();
            final DataSerializableFactory f = new DataSerializableFactory() {
                public IdentifiedDataSerializable create(int typeId) {
                    return new IDSSampleObject();
                }
            };

            public byte[] write(Object o) throws IOException {
                BufferObjectDataOutput out = ss.createObjectDataOutput(BUFFER_SIZE);
                IdentifiedDataSerializable ds = (IdentifiedDataSerializable) o;
                out.writeInt(ds.getFactoryId());
                out.writeInt(ds.getId());
                ds.writeData(out);
                return out.toByteArray();
            }

            public Object read(byte[] data) throws IOException, ClassNotFoundException {
                BufferObjectDataInput in = ss.createObjectDataInput(data);
                in.readInt();
                int type = in.readInt();
                DataSerializable ds = f.create(type);
                ds.readData(in);
                return ds;
            }
        };

        final Factory factory = new Factory() {
            SampleObject create(int i) {
                return new IDSSampleObject(i);
            }
        };

        test(serializer, "IdentifiedDataSerializable", COUNT, factory);
    }

    private static void testKryo() throws Exception {
        final Factory factory = new Factory() {
            SampleObject create(int i) {
                return new SampleObject(i);
            }
        };

        final Serializer serializer = new Serializer() {
            final Kryo kryo = new Kryo();

            public byte[] write(Object o) throws IOException {
                Output output = new Output(new byte[BUFFER_SIZE]);
                kryo.writeClassAndObject(output, o);
                output.flush();
                return output.toBytes();
            }

            public Object read(byte[] data) throws IOException, ClassNotFoundException {
                Input input = new Input(data);
                return kryo.readClassAndObject(input);
            }
        };

        test(serializer, "Kryo", COUNT, factory);
    }

    private static void testKryoUnsafe() throws Exception {
        final Factory factory = new Factory() {
            SampleObject create(int i) {
                return new SampleObject(i);
            }
        };

        final Serializer serializer = new Serializer() {
            final Kryo kryo = new Kryo();

            public byte[] write(Object o) throws IOException {
                Output output = new UnsafeOutput(new byte[BUFFER_SIZE]);
                kryo.writeClassAndObject(output, o);
                output.flush();
                return output.toBytes();
            }

            public Object read(byte[] data) throws IOException, ClassNotFoundException {
                Input input = new UnsafeInput(data);
                return kryo.readClassAndObject(input);
            }
        };

        test(serializer, "Kryo Unsafe", COUNT, factory);
    }

    private static void test(Serializer serializer, String type, int count, Factory factory) throws Exception {
        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            Object so = factory.createAndSetValues(i);
            final byte[] data = serializer.write(so);
            Object newObject = serializer.read(data);

            if (newObject == null) {
                throw new NullPointerException();
            }
            if (!so.equals(newObject)) {
                 throw new IllegalArgumentException();
            }
        }
        long end = System.currentTimeMillis();
        long total = (end - start);
        System.out.println(type + ":: " + total + " ms");
        gc();
    }

    interface Serializer {

        byte[] write(Object o) throws IOException;

        Object read(byte[] data) throws IOException, ClassNotFoundException;
    }
}

