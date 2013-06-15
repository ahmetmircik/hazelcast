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
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class SampleObject
        implements java.io.Serializable, KryoSerializable {
    public int intVal;
    public float floatVal;
    public Short shortVal;
    public long[] longArr;
    public double[] dblArr;
//    public SampleObject selfRef;

    public SampleObject() {
    }

    public SampleObject(int intVal) {
        this.intVal = intVal;
    }

    SampleObject(int intVal, float floatVal, Short shortVal,
                 long[] longArr, double[] dblArr) {
        this.intVal = intVal;
        this.floatVal = floatVal;
        this.shortVal = shortVal;
        this.longArr = longArr;
        this.dblArr = dblArr;
//        selfRef = this;
    }
    // Required by Java Externalizable.

        public void writeExternal(ObjectOutput out)
            throws IOException {
        out.writeInt(intVal);
        out.writeFloat(floatVal);
        out.writeShort(shortVal);
        out.writeObject(longArr);
        out.writeObject(dblArr);
    }

    // Required by Java Externalizable.
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        intVal = in.readInt();
        floatVal = in.readFloat();
        shortVal = in.readShort();
        longArr = (long[]) in.readObject();
        dblArr = (double[]) in.readObject();
    }

    // Required by Kryo serialization.
    public void write(Kryo kryo, Output out) {
        kryo.writeObject(out, intVal);
        kryo.writeObject(out, floatVal);
        kryo.writeObject(out, shortVal);
        kryo.writeObject(out, longArr);
        kryo.writeObject(out, dblArr);
//        kryo.writeObject(out, selfRef);
        out.flush();
    }

    // Required by Kryo serialization.
    public void read(Kryo kryo, Input in) {
        intVal = kryo.readObject(in, Integer.class);
        floatVal = kryo.readObject(in, Float.class);
        shortVal = kryo.readObject(in, Short.class);
        longArr = kryo.readObject(in, long[].class);
        dblArr = kryo.readObject(in, double[].class);
//        selfRef = kryo.readObject(in, SampleObject.class);
    }
}
