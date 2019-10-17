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

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.impl.Metadata;

import static com.hazelcast.internal.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.internal.util.JVMUtil.REFERENCE_COST_IN_BYTES;

public class MinimalDataRecord implements Record<Data> {

    private static final int NUMBER_OF_LONGS = 1;
    private static final int NUMBER_OF_REFERENCES = 2;

    protected Data key;
    protected Data value;
    protected long version;

    @Override
    public Data getKey() {
        return key;
    }

    @Override
    public void setKey(Data key) {
        this.key = key;
    }

    @Override
    public Data getValue() {
        return value;
    }

    @Override
    public void setValue(Data value) {
        this.value = value;
    }

    @Override
    public long getCost() {
        return NUMBER_OF_REFERENCES * REFERENCE_COST_IN_BYTES
                + (NUMBER_OF_LONGS * LONG_SIZE_IN_BYTES)
                + (value == null ? 0 : value.getHeapCost());
    }

    @Override
    public long getVersion() {
        return version;
    }

    @Override
    public void setVersion(long version) {
        this.version = version;
    }

    @Override
    public Object getCachedValueUnsafe() {
        return Record.NOT_CACHED;
    }

    @Override
    public boolean casCachedValue(Object expectedValue, Object newValue) {
        return true;
    }

    @Override
    public long getLastAccessTime() {
        return NOT_AVAILABLE;
    }

    @Override
    public void setLastAccessTime(long lastAccessTime) {

    }

    @Override
    public long getLastUpdateTime() {
        return NOT_AVAILABLE;
    }

    @Override
    public void setLastUpdateTime(long lastUpdatedTime) {

    }

    @Override
    public long getCreationTime() {
        return 0;
    }

    @Override
    public void setCreationTime(long creationTime) {

    }

    @Override
    public int getHits() {
        return NOT_AVAILABLE;
    }

    @Override
    public void setHits(int hits) {

    }

    @Override
    public long getExpirationTime() {
        return NOT_AVAILABLE;
    }

    @Override
    public void setExpirationTime(long expirationTime) {

    }

    @Override
    public long getLastStoredTime() {
        return NOT_AVAILABLE;
    }

    @Override
    public void setLastStoredTime(long lastStoredTime) {

    }

    @Override
    public long getSequence() {
        return NOT_AVAILABLE;
    }

    @Override
    public void setSequence(long sequence) {

    }

    @Override
    public void setMetadata(Metadata metadata) {

    }

    @Override
    public Metadata getMetadata() {
        return null;
    }

    @Override
    public RecordReaderWriter getMatchingRecordReaderWriter() {
        return null;
    }

    @Override
    public long getTtl() {
        return Long.MAX_VALUE;
    }

    @Override
    public void setTtl(long ttl) {

    }

    @Override
    public long getMaxIdle() {
        return Long.MAX_VALUE;
    }

    @Override
    public void setMaxIdle(long maxIdle) {

    }

    @Override
    public int getRawTtl() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getRawMaxIdle() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getRawCreationTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getRawLastAccessTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getRawLastUpdateTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setRawTtl(int readInt) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setRawMaxIdle(int readInt) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setRawCreationTime(int readInt) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setRawLastAccessTime(int readInt) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setRawLastUpdateTime(int readInt) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getRawLastStoredTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setRawLastStoredTime(int time) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getRawExpirationTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setRawExpirationTime(int time) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "MinimalDataRecord{"
                + "key=" + key
                + ", value=" + value
                + ", version=" + version
                + '}';
    }
}
