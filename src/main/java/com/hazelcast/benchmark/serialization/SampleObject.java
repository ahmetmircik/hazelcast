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

import java.util.Arrays;

public class SampleObject implements java.io.Serializable, KryoSerializable {

    public int intVal;
    public float floatVal;
    public short shortVal;
    public byte[] byteArr;
    public long[] longArr;
    public double[] dblArr;
    public String str;


    public SampleObject() {
    }

    public SampleObject(int intVal) {
        this.intVal = intVal;
    }

    // Required by Kryo serialization.
    public void write(Kryo kryo, Output out) {
        kryo.writeObject(out, intVal);
        kryo.writeObject(out, floatVal);
        kryo.writeObject(out, shortVal);
        kryo.writeObject(out, byteArr);
        kryo.writeObject(out, longArr);
        kryo.writeObject(out, dblArr);
        kryo.writeObject(out, str);
        out.flush();
    }

    // Required by Kryo serialization.
    public void read(Kryo kryo, Input in) {
        intVal = kryo.readObject(in, Integer.class);
        floatVal = kryo.readObject(in, Float.class);
        shortVal = kryo.readObject(in, Short.class);
        byteArr = kryo.readObject(in, byte[].class);
        longArr = kryo.readObject(in, long[].class);
        dblArr = kryo.readObject(in, double[].class);
        str = kryo.readObject(in, String.class);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SampleObject that = (SampleObject) o;

        if (Float.compare(that.floatVal, floatVal) != 0) return false;
        if (intVal != that.intVal) return false;
        if (shortVal != that.shortVal) return false;
        if (!Arrays.equals(byteArr, that.byteArr)) return false;
        if (!Arrays.equals(dblArr, that.dblArr)) return false;
        if (!Arrays.equals(longArr, that.longArr)) return false;
        if (str != null ? !str.equals(that.str) : that.str != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = intVal;
        result = 31 * result + (floatVal != +0.0f ? Float.floatToIntBits(floatVal) : 0);
        result = 31 * result + (int) shortVal;
        result = 31 * result + (byteArr != null ? Arrays.hashCode(byteArr) : 0);
        result = 31 * result + (longArr != null ? Arrays.hashCode(longArr) : 0);
        result = 31 * result + (dblArr != null ? Arrays.hashCode(dblArr) : 0);
        result = 31 * result + (str != null ? str.hashCode() : 0);
        return result;
    }
}
