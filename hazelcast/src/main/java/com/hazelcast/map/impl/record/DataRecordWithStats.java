/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.record;

import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;

import static com.hazelcast.util.JVMUtil.REFERENCE_COST_IN_BYTES;

public class DataRecordWithStats extends AbstractRecordWithStats<Data> {

    protected volatile Data value;

    public DataRecordWithStats() {
    }

    public DataRecordWithStats(Data value) {
        super();
        this.value = value;
    }

    /**
     * Get record size in bytes.
     */
    @Override
    public long getCost() {
        return super.getCost() + REFERENCE_COST_IN_BYTES + (value == null ? 0L : value.getHeapCost());
    }

    @Override
    public Data getValue() {
        return value;
    }

    @Override
    public void setValue(Data o) {
        value = o;
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }

        DataRecordWithStats that = (DataRecordWithStats) o;

        return value.equals(that.value);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.DATA_RECORD_WITH_STATS;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);

        out.writeData(value);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);

        value = in.readData();
    }
}
