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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class DSSampleObject extends SampleObject implements DataSerializable {

    public DSSampleObject() {
    }

    public DSSampleObject(int intVal) {
        super(intVal);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(intVal);
        out.writeShort(shortVal);
        out.writeFloat(floatVal);
        out.writeInt(dblArr.length);

        for (int i = 0; i < dblArr.length; i++) {
            out.writeDouble(dblArr[i]);
        }
        out.writeInt(longArr.length);
        for (int i = 0; i < longArr.length; i++) {
            out.writeDouble(longArr[i]);
        }

    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        intVal = in.readInt();
        shortVal = in.readShort();
        floatVal = in.readFloat();
        dblArr = new double[in.readInt()];
        for (int i = 0; i < dblArr.length; i++) {
            dblArr[i] = in.readDouble();
        }
        longArr = new long[in.readInt()];
        for (int i = 0; i < longArr.length; i++) {
            longArr[i] = in.readLong();
        }
    }
}
