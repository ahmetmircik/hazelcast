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
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.serialization.SerializationServiceBuilder;

public class Benchmark2 {

    static final int COUNT = 60000;

    public static void main(String[] args) throws Exception {
        testJDK();
        testDS();
        testIDS();
        testPortable();
        testKryo();
        testKryoUnsafe();
    }

    private static void testJDK() throws Exception {
        test(new SerializationConfig(), "Java Serialization", COUNT, 3, new QuickSearchFactory());
    }

    private static void testDS() throws Exception {
        test(new SerializationConfig(), "DataSerializable", COUNT, 3, new QuickSearchFactory());
    }

    private static void testIDS() throws Exception {
        SerializationConfig config = new SerializationConfig();
        config.addDataSerializableFactory(1, new DataSerializableFactory() {
            QuickSearchFactory factory = new QuickSearchFactory();

            public IdentifiedDataSerializable create(int typeId) {
                return (IDSQuickSearch)factory.createQuickSearchObject();
            }
        });

        test(config, "IdentifiedDataSerializable", COUNT, 3, new QuickSearchFactory());
    }

    private static void testPortable() throws Exception {
        SerializationConfig config = new SerializationConfig();
        config.addPortableFactory(PortableFactoryImpl.FACTORY_ID, new PortableFactoryImpl());

        test(config, "Portable", COUNT, 3, new QuickSearchFactory());
    }

    private static void testKryo() throws Exception {
        SerializationConfig config = new SerializationConfig();
        config.addSerializerConfig(new SerializerConfig().
                setTypeClass(KryoSerializable.class).
                setImplementation(new QuickSearchKryoSerializer(false)));

        test(config, "Kryo", COUNT, 3, new QuickSearchFactory());
    }

    private static void testKryoUnsafe() throws Exception {
        SerializationConfig config = new SerializationConfig();
        config.addSerializerConfig(new SerializerConfig().
                setTypeClass(KryoSerializable.class).
                setImplementation(new QuickSearchKryoSerializer(true)));

        test(config, "Kryo-unsafe", COUNT, 3, new QuickSearchFactory());
    }

    private static void test(SerializationConfig config, String type, int count, int iteration, QuickSearchFactory factory) throws Exception {
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

    private static int iterate(int count, QuickSearchFactory factory, SerializationService ss, boolean warmup) {
        int bufferSize = 0;
        for (int i = 0; i < count; i++) {
            Object so = factory.createQuickSearchObject();
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

