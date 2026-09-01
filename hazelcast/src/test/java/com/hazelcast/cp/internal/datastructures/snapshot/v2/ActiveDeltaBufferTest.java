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
import com.hazelcast.cp.internal.datastructures.snapshot.v2.engine.ActiveDeltaBuffer;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ActiveDeltaBufferTest {

    @Test
    public void testAppendAndDetach() {
        ActiveDeltaBuffer buffer = new ActiveDeltaBuffer(10);
        assertEquals(0, buffer.size());
        assertEquals(10, buffer.capacity());

        DeltaEntry e1 = new DeltaEntry((short) 1, null, 1L, DeltaEntry.OP_PUT, null, null, 100L, 0);
        DeltaEntry e2 = new DeltaEntry((short) 1, null, 1L, DeltaEntry.OP_PUT, null, null, 101L, 0);

        buffer.append(e1);
        buffer.append(e2);

        assertEquals(2, buffer.size());
        assertTrue(buffer.totalBytes() > 0);

        FrozenDeltaSegment segment = buffer.detach(1L, 100L, 101L, 1);
        assertEquals(1L, segment.sequence());
        assertEquals(100L, segment.fromRaftIndex());
        assertEquals(101L, segment.toRaftIndex());
        assertEquals(2, segment.entryCount());

        Iterator<DeltaEntry> it = segment.iterator();
        assertTrue(it.hasNext());
        assertEquals(e1, it.next());
        assertTrue(it.hasNext());
        assertEquals(e2, it.next());
        assertFalse(it.hasNext());
    }

    @Test(expected = IllegalStateException.class)
    public void testCapacityExceededThrows() {
        ActiveDeltaBuffer buffer = new ActiveDeltaBuffer(1);
        buffer.append(new DeltaEntry((short) 1, null, 1L, DeltaEntry.OP_PUT, null, null, 1L, 0));
        buffer.append(new DeltaEntry((short) 1, null, 1L, DeltaEntry.OP_PUT, null, null, 2L, 0));
    }
}
