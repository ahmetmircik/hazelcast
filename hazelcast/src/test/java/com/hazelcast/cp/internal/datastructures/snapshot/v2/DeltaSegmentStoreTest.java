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

package com.hazelcast.cp.internal.datastructures.snapshot.v2;

import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.DeltaEntry;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.FrozenDeltaSegment;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.engine.DeltaSegmentStore;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DeltaSegmentStoreTest {

    @Test
    public void testAppendAndAcquireRange() {
        DeltaSegmentStore store = new DeltaSegmentStore(1L);
        assertTrue(store.availableSlots() > 1000);

        FrozenDeltaSegment seg1 = new FrozenDeltaSegment(1L, 10L, 20L, 1, 1, 100L, new DeltaEntry[1]);
        FrozenDeltaSegment seg2 = new FrozenDeltaSegment(2L, 21L, 30L, 1, 1, 100L, new DeltaEntry[1]);

        store.appendSegment(seg1);
        store.appendSegment(seg2);

        assertEquals(2L, store.currentTailSequence());
        assertEquals(seg1, store.getSegment(1L));
        assertEquals(seg2, store.getSegment(2L));

        List<FrozenDeltaSegment> range = store.acquireRange(1L, 2L);
        assertEquals(2, range.size());
        assertEquals(seg1, range.get(0));
        assertEquals(seg2, range.get(1));
    }

    @Test
    public void testAdvanceReclamationFloorAndClean() {
        DeltaSegmentStore store = new DeltaSegmentStore(1L);
        FrozenDeltaSegment seg1 = new FrozenDeltaSegment(1L, 10L, 20L, 1, 1, 100L, new DeltaEntry[1]);
        store.appendSegment(seg1);

        store.advanceReclamationFloor(2L);
        assertEquals(2L, store.reclamationFloor());

        store.cleanRetiredSlots();
        assertEquals(null, store.getSegment(1L));
    }
}
