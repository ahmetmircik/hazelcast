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

import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RecordsTest extends HazelcastTestSupport {

    private SerializationService serializationService;

    @Before
    public void setUp() {
        serializationService = new DefaultSerializationServiceBuilder().build();
    }

    @Test
    public void getValueOrCachedValue_whenRecordIsNotCachable_thenDoNotCache() {
        String objectPayload = "foo";
        Data dataPayload = serializationService.toData(objectPayload);
        Record record = new DataRecord(dataPayload);
        Object value = Records.getValueOrCachedValue(record, null);
        assertSame(dataPayload, value);
    }

    @Test
    public void getValueOrCachedValue_whenRecordIsCachedDataRecordWithStats_thenCache() {
        String objectPayload = "foo";
        Data dataPayload = serializationService.toData(objectPayload);
        Record record = new CachedDataRecordWithStats(dataPayload);
        Object firstDeserializedValue = Records.getValueOrCachedValue(record, serializationService);
        assertEquals(objectPayload, firstDeserializedValue);

        // we don't need serialization service for the 2nd call
        Object secondDeserializedValue = Records.getValueOrCachedValue(record, null);
        assertSame(firstDeserializedValue, secondDeserializedValue);
    }

    @Test
    public void getValueOrCachedValue_whenRecordIsCachedDataRecord_thenCache() {
        String objectPayload = "foo";
        Data dataPayload = serializationService.toData(objectPayload);
        Record record = new CachedDataRecord(dataPayload);
        Object firstDeserializedValue = Records.getValueOrCachedValue(record, serializationService);
        assertEquals(objectPayload, firstDeserializedValue);

        // we don't need serialization service for the 2nd call
        Object secondDeserializedValue = Records.getValueOrCachedValue(record, null);
        assertSame(firstDeserializedValue, secondDeserializedValue);
    }

    @Test
    public void givenCachedDataRecord_whenThreadIsInside_thenGetValueOrCachedValueReturnsTheThread() {
        // given
        CachedDataRecord record = new CachedDataRecord();

        // when
        SerializableThread objectPayload = new SerializableThread();
        Data dataPayload = serializationService.toData(objectPayload);
        record.setValue(dataPayload);

        // then
        Object cachedValue = Records.getValueOrCachedValue(record, serializationService);
        assertInstanceOf(SerializableThread.class, cachedValue);
    }

    @Test
    public void givenCachedDataRecordValueIsThread_whenCachedValueIsCreated_thenGetCachedValueReturnsTheThread() {
        // given
        SerializableThread objectPayload = new SerializableThread();
        Data dataPayload = serializationService.toData(objectPayload);
        CachedDataRecord record = new CachedDataRecord(dataPayload);

        // when
        Records.getValueOrCachedValue(record, serializationService);

        // then
        Object cachedValue = Records.getCachedValue(record);
        assertInstanceOf(SerializableThread.class, cachedValue);
    }

    @Test
    public void applyRecordInfo() {
        // Shared key&value by referenceRecord and fromRecord-applied-toRecord
        Data key = new HeapData();
        Data value = new HeapData();

        // Create fromRecord from a reference record
        Record referenceRecord = new DataRecordWithStats();
        referenceRecord.setKey(key);
        referenceRecord.setValue(value);
        referenceRecord.setHits(123);
        referenceRecord.setVersion(12);
        Record fromRecord = toRecordInfo(referenceRecord);

        // Apply created fromRecord to toRecord
        Record toRecord = new DataRecordWithStats();
        toRecord.setKey(key);
        toRecord.setValue(value);

        Records.applyRecordInfo(fromRecord, toRecord);

        // Check fromRecord applied correctly to toRecord
        assertEquals(referenceRecord, toRecord);
    }

    private static Record toRecordInfo(Record record) {
        Record recordInfo = mock(Record.class);

        when(recordInfo.getCreationTime()).thenReturn(record.getCreationTime());
        when(recordInfo.getLastAccessTime()).thenReturn(record.getLastAccessTime());
        when(recordInfo.getLastUpdateTime()).thenReturn(record.getLastUpdateTime());
        when(recordInfo.getHits()).thenReturn(record.getHits());
        when(recordInfo.getVersion()).thenReturn(record.getVersion());
        when(recordInfo.getExpirationTime()).thenReturn(record.getExpirationTime());
        when(recordInfo.getLastStoredTime()).thenReturn(record.getLastStoredTime());

        return recordInfo;
    }

    private static class SerializableThread extends Thread implements Serializable {
    }
}
