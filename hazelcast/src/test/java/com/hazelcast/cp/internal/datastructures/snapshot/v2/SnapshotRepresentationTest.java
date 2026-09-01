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

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.BaseRef;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.CanonicalSnapshotHeader;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.Hash256;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.RepresentationState;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.SnapshotRepresentationId;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.engine.SnapshotHandle;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.engine.SnapshotRepresentation;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SnapshotRepresentationTest {

    @Test
    public void testLifecycleAndHandleAcquisition() {
        CPGroupId groupId = new RaftGroupId("test-group", 1L, 1L);
        SnapshotRepresentationId id = new SnapshotRepresentationId(groupId, 1L, 1L, 100L);
        SnapshotRepresentation rep = new SnapshotRepresentation(id, BaseRef.empty(), List.of(), RepresentationState.UNSEALED);

        assertEquals(RepresentationState.UNSEALED, rep.state());
        assertEquals(1, rep.activeReferences());

        try (SnapshotHandle handle = rep.tryAcquireHandle()) {
            assertNotNull(handle);
            assertEquals(2, rep.activeReferences());
        }
        assertEquals(1, rep.activeReferences());

        CanonicalSnapshotHeader header = new CanonicalSnapshotHeader(
            groupId, 1L, 100L, 1, 50L, List.of(), Map.of(), 0L, 0L, 0L, 0L, List.of(), List.of(new Hash256(new byte[32]))
        );
        assertTrue(rep.seal(header));
        assertEquals(RepresentationState.SEALED, rep.state());
        assertNotNull(rep.metadata());

        rep.retire();
        assertEquals(RepresentationState.RETIRED, rep.state());
        assertNull("Cannot acquire handle after retirement", rep.tryAcquireHandle());
    }
}
