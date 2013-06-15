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

import com.esotericsoftware.kryo.KryoSerializable;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.TypeSerializerConfig;
import com.hazelcast.nio.serialization.*;

public class Benchmark {

    static final int COUNT = 10000;

    public static void main(String[] args) throws Exception {
        testJDK();
        testDS();
        testIDS();
        testPortable();
        testKryo();
        testKryoUnsafe();
    }

    private static void testJDK() throws Exception {
        test(new SerializationConfig(), "Java Serialization", COUNT, 3, new Factory() {
            SampleObject create(int i) {
                return new SampleObject(i);
            }
        });
    }

    private static void testDS() throws Exception {
        test(new SerializationConfig(), "DataSerializable", COUNT, 3, new Factory() {
            SampleObject create(int i) {
                return new DSSampleObject(i);
            }
        });
    }

    private static void testIDS() throws Exception {
        SerializationConfig config = new SerializationConfig();
        config.addDataSerializableFactory(1, new DataSerializableFactory() {
            public IdentifiedDataSerializable create(int typeId) {
                return new IDSSampleObject();
            }
        });

        test(config, "IdentifiedDataSerializable", COUNT, 3, new Factory() {
            SampleObject create(int i) {
                return new IDSSampleObject(i);
            }
        });
    }

    private static void testPortable() throws Exception {
        SerializationConfig config = new SerializationConfig();
        config.addPortableFactory(1, new PortableFactory() {
            public Portable create(int classId) {
                return new PortableSampleObject();
            }
        });

        test(config, "Portable", COUNT, 3, new Factory() {
            SampleObject create(int i) {
                return new PortableSampleObject(i);
            }
        });
    }

    private static void testKryo() throws Exception {
        SerializationConfig config = new SerializationConfig();
        config.addTypeSerializer(new TypeSerializerConfig().
                setTypeClass(KryoSerializable.class).
                setImplementation(new SOKryoSerializer(false)));

        test(config, "Kryo", COUNT, 3, new Factory() {
            SampleObject create(int i) {
                return new SampleObject(i);
            }
        });
    }

    private static void testKryoUnsafe() throws Exception {
        SerializationConfig config = new SerializationConfig();
        config.addTypeSerializer(new TypeSerializerConfig().
                setTypeClass(KryoSerializable.class).
                setImplementation(new SOKryoSerializer(true)));

        test(config, "Kryo-unsafe", COUNT, 3, new Factory() {
            SampleObject create(int i) {
                return new SampleObject(i);
            }
        });
    }

    private static void test(SerializationConfig config, String type, int count, int iteration, Factory factory) throws Exception {
        SerializationService ss = new SerializationServiceBuilder().setConfig(config).setUseNativeByteOrder(true).build();
        int bufferSize = iterate(500, factory, ss, true);
        System.gc();
        Thread.sleep(100);
        long total = 0L;
        for (int j = 0; j < iteration; j++) {
            long start = System.currentTimeMillis();
            iterate(count, factory, ss, false);
            long end = System.currentTimeMillis();
            total += (end - start);
        }
        System.out.println(type + ":: " + bufferSize + " bytes :: " + total / iteration + " ms");
    }

    private static int iterate(int count, Factory factory, SerializationService ss, boolean warmup) {
        int bufferSize = 0;
        for (int i = 0; i < count; i++) {
            Object so = factory.createAndSetValues(i);
            Data data = ss.toData(so);
            Object newObject = ss.toObject(data);
            if (newObject == null) {
                throw new NullPointerException();
            }
//            if (!so.equals(newObject)) {
//                 throw new IllegalArgumentException();
//            }
            if (warmup) {
                bufferSize = Math.max(bufferSize, data.bufferSize());
            }
        }
        return bufferSize;
    }


}

