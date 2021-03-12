/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.serialization.Data;

import static com.hazelcast.internal.util.JVMUtil.REFERENCE_COST_IN_BYTES;
import static com.hazelcast.map.impl.record.RecordReaderWriter.SIMPLE_DATA_RECORD_WITH_STATS_READER_WRITER;

class SimpleRecordWithStats<V> extends AbstractRecord<V> {
    protected volatile V value;

    SimpleRecordWithStats() {
    }

    SimpleRecordWithStats(V value) {
        setValue(value);
    }

    @Override
    public RecordReaderWriter getMatchingRecordReaderWriter() {
        return SIMPLE_DATA_RECORD_WITH_STATS_READER_WRITER;
    }

    /**
     * Get record size in bytes.
     */
    @Override
    public long getCost() {
        if (value instanceof Data) {
            return super.getCost() + REFERENCE_COST_IN_BYTES
                    + (value == null ? 0L : ((Data) value).getHeapCost());
        } else {
            return 0;
        }
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public void setValue(V o) {
        value = o;
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }

        SimpleRecordWithStats that = (SimpleRecordWithStats) o;
        return value.equals(that.value);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "SimpleRecordWithStats{"
                + "value=" + value
                + ", " + super.toString()
                + "} ";
    }
}
