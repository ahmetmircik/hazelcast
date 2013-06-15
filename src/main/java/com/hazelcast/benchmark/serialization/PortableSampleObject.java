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

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

public class PortableSampleObject extends SampleObject implements Portable {
    public PortableSampleObject() {
    }

    public PortableSampleObject(int intVal) {
        super(intVal);
    }

    @Override
    public int getFactoryId() {
        return 1;
    }

    @Override
    public int getClassId() {
        return 1;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeInt("intV", intVal);
        writer.writeFloat("floatV", floatVal);
        writer.writeShort("shortV", shortVal);
        writer.writeByteArray("byteArr", byteArr);
        writer.writeLongArray("longArr", longArr);
        writer.writeDoubleArray("dblArr", dblArr);
        writer.writeUTF("str", str);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        intVal = reader.readInt("intV");
        floatVal = reader.readFloat("floatV");
        shortVal = reader.readShort("shortV");
        byteArr = reader.readByteArray("byteArr");
        longArr = reader.readLongArray("longArr");
        dblArr = reader.readDoubleArray("dblArr");
        str = reader.readUTF("str");
    }
}
