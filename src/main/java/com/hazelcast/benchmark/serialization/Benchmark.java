/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.benchmark.serialization;

/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.TypeSerializerConfig;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.serialization.SerializationServiceBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class Benchmark {

    private static Random random = new Random(System.currentTimeMillis());

    public static void main(String[] args) {
        int count = 10000;
        SerializationConfig config = new SerializationConfig();
        config.addPortableFactory
                (1, (int i) -> (i == 1) ? new PortableSampleObject() : null);
        config.addDataSerializableFactory
                (1, (int i) -> (i == 1) ? new IDSSampleObject() : null);
        test(config, "Java Serialization", count, 3, (int i) -> new SampleObject(i));
        config.addTypeSerializer(
                new TypeSerializerConfig().
                        setTypeClass(SampleObject.class).
                        setImplementation(new SOKryoSerializer(false)));
        test(config, "Portable", count, 3, (int i) -> new PortableSampleObject(i));
        test(config, "DataSerializable", count, 3, (int i) -> new DSSampleObject(i));
        test(config, "IdentifiedDataSerializable", count, 3, (int i) -> new IDSSampleObject(i));
        test(config, "Kryo", count, 3, (int i) -> new SampleObject(i));
//        config.getTypeSerializers().iterator().next().setImplementation(new SOSmileSerializer());
//        test(config, "Smile", count, 3, (int i) -> new SampleObject(i));
        config.getTypeSerializers().iterator().next().setImplementation(new SOKryoSerializer(true));
        test(config, "Kryo-unsafe", count, 3, (int i) -> new SampleObject(i));
    }

    private static void test(SerializationConfig config, String type, int count, int iteration, Factory factory) {
        SerializationService ss = new SerializationServiceBuilder().setConfig(config).build();
        int bufferSize = iterate(1000, factory, ss, true, type);
        List<Long> result = new ArrayList<>(iteration);
        for (int j = 0; j < iteration; j++) {
            long start = System.currentTimeMillis();
            iterate(count, factory, ss, false, type);
            long end = System.currentTimeMillis();
            result.add(end - start);
        }
        final AtomicLong total = new AtomicLong(0);
        result.forEach((Long l) -> total.addAndGet(l));
        System.out.println(type + ":: " + bufferSize + " bytes :: " + total.get() / iteration + " ms");
    }

    private static int iterate(int count, Factory factory, SerializationService ss, boolean warmup, String type) {
        int bufferSize = 0;
        for (int i = 0; i < count; i++) {
            Object so = factory.createAndSetValues(i);
            Data data = ss.toData(so);
            Object newObject = ss.toObject(data);
            if (newObject == null) throw new NullPointerException();
            if (i == 0 && warmup)
                bufferSize = data.bufferSize();
        }
        return bufferSize;
    }


    public interface Factory {
        default Object createAndSetValues(int id) {
            long[] longArr = new long[3000];
            for (int i = 0; i < longArr.length; i++)
                longArr[i] = i;
            double[] dblArr = new double[3000];
            for (int i = 0; i < dblArr.length; i++)
                dblArr[i] = 0.1 * i;
            SampleObject object = create(id);
            object.longArr = longArr;
            object.dblArr = dblArr;
            object.shortVal = (short) 321;
            object.floatVal = 123.456f;
            return object;
        }

        SampleObject create(int intVal);
    }
}

