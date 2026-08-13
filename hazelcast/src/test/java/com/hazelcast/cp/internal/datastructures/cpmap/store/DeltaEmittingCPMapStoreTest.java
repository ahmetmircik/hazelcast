/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.cpmap.store;

import com.hazelcast.cp.internal.datastructures.cpmap.CPMapServiceUtil;
import com.hazelcast.cp.internal.datastructures.snapshot.DeltaContext;
import com.hazelcast.cp.internal.datastructures.snapshot.DeltaRecord;
import com.hazelcast.cp.internal.datastructures.snapshot.SnapshotDeltaAccumulator;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static com.hazelcast.cp.internal.datastructures.snapshot.DeltaRecord.Kind.DELETE;
import static com.hazelcast.cp.internal.datastructures.snapshot.DeltaRecord.Kind.PUT;
import static java.util.List.of;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the delta-exactness contract of {@link DeltaEmittingCPMapStore}
 * (design test F2): emissions reflect the ACTUAL outcome of each store call,
 * conditional ops emit only on success, and no emission happens without an
 * active {@link DeltaContext} (the restore/install case).
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DeltaEmittingCPMapStoreTest {

    private static final String MAP_NAME = "test-map";
    private static final long TS = 1234L;

    private HeapCPMapStore delegate;
    private SnapshotDeltaAccumulator sink;
    private DeltaEmittingCPMapStore store;

    @Before
    public void before() {
        delegate = new HeapCPMapStore(1);
        sink = new SnapshotDeltaAccumulator(1024);
        store = new DeltaEmittingCPMapStore(CPMapServiceUtil.SERVICE_NAME, MAP_NAME, delegate);
    }

    @Test
    public void put_emitsPutDeltaWithTimestampAndOpIndex() {
        Data key = getData();
        Data value = getData();

        applyAt(42L, () -> store.put(key, value, TS));

        List<DeltaRecord> deltas = sink.drain();
        assertEquals(1, deltas.size());
        assertEquals(new DeltaRecord(PUT, CPMapServiceUtil.SERVICE_NAME, MAP_NAME, key, value, TS, 42L), deltas.get(0));
        assertEquals(value, delegate.get(key));
    }

    @Test
    public void putIfAbsent_emitsOnlyWhenKeyWasAbsent() {
        Data key = getData();
        Data v1 = getData();
        Data v2 = getData();

        applyAt(1L, () -> assertTrue(store.putIfAbsent(key, v1, TS) == null));
        assertEquals(1, sink.drain().size());

        // second attempt: key present -> no delta
        applyAt(2L, () -> assertEquals(v1, store.putIfAbsent(key, v2, TS)));
        assertTrue(sink.drain().isEmpty());
    }

    @Test
    public void compareAndSet_emitsOnlyWhenChanged() {
        Data key = getData();
        Data v1 = getData();
        Data v2 = getData();

        applyAt(1L, () -> store.put(key, v1, TS));
        sink.drain();

        // matching expected value -> changed -> delta
        applyAt(2L, () -> assertTrue(store.compareAndSet(key, v1, v2, TS)));
        assertEquals(1, sink.drain().size());

        // mismatching expected value -> not changed -> no delta
        applyAt(3L, () -> assertTrue(!store.compareAndSet(key, v1, v2, TS)));
        assertTrue(sink.drain().isEmpty());
    }

    @Test
    public void remove_emitsOnlyWhenKeyExisted() {
        Data key = getData();

        // absent key -> no delta
        applyAt(1L, () -> assertTrue(store.remove(key) == null));
        assertTrue(sink.drain().isEmpty());

        applyAt(2L, () -> store.put(key, getData(), TS));
        sink.drain();

        // present key -> delete delta
        applyAt(3L, () -> assertTrue(store.remove(key) != null));
        List<DeltaRecord> deltas = sink.drain();
        assertEquals(1, deltas.size());
        assertEquals(new DeltaRecord(DELETE, CPMapServiceUtil.SERVICE_NAME, MAP_NAME, key, null, -1L, 3L), deltas.get(0));
    }

    @Test
    public void noEmissionWithoutActiveContext() {
        // restore/install case: the coordinator never opens a context there
        store.put(getData(), getData(), TS);
        store.remove(getData());
        assertTrue(sink.drain().isEmpty());
    }

    @Test
    public void purge_emitsDeletePerRemovedKey() {
        CPMapStore mockDelegate = mock(CPMapStore.class);
        Data k1 = getData();
        Data k2 = getData();
        when(mockDelegate.purge(1000L, TS)).thenReturn(of(k1, k2));

        DeltaEmittingCPMapStore purgeStore = new DeltaEmittingCPMapStore(CPMapServiceUtil.SERVICE_NAME, MAP_NAME, mockDelegate);
        applyAt(7L, () -> purgeStore.purge(1000L, TS));

        List<DeltaRecord> deltas = sink.drain();
        assertEquals(2, deltas.size());
        assertEquals(new DeltaRecord(DELETE, CPMapServiceUtil.SERVICE_NAME, MAP_NAME, k1, null, -1L, 7L), deltas.get(0));
        assertEquals(new DeltaRecord(DELETE, CPMapServiceUtil.SERVICE_NAME, MAP_NAME, k2, null, -1L, 7L), deltas.get(1));
    }

    @Test
    public void readMethodsDelegateUntouched() {
        Data key = getData();
        Data value = getData();
        delegate.put(key, value, TS);

        assertEquals(value, store.get(key));
        assertEquals(1, store.size());
        assertTrue(!store.isPurgeEnabled());
    }

    private void applyAt(long opIndex, Runnable mutation) {
        sink.beginBatch(opIndex);
        try (DeltaContext ignored = DeltaContext.begin(sink, opIndex)) {
            mutation.run();
        }
    }

    private static Data getData() {
        return mock(Data.class);
    }
}
