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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.UnsafeInput;
import com.esotericsoftware.kryo.io.UnsafeOutput;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.TypeSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class SOKryoSerializer implements TypeSerializer<Object> {

    Kryo kryo = new Kryo();
    final boolean unsafe;

    public SOKryoSerializer(boolean unsafe) {
        this.unsafe = unsafe;
        kryo.register(SampleObject.class);
    }

    public int getTypeId() {
        return 9;
    }

    public void write(ObjectDataOutput out, Object object) throws IOException {
        Output output = unsafe ? new UnsafeOutput((OutputStream) out) : new Output((OutputStream) out);
        kryo.writeClassAndObject(output, object);
        output.flush();
    }

    public Object read(ObjectDataInput in) throws IOException {
        Input input = unsafe ? new UnsafeInput((InputStream) in) : new Input((InputStream) in);
        return kryo.readClassAndObject(input);
    }

    public void destroy() {
    }
}
