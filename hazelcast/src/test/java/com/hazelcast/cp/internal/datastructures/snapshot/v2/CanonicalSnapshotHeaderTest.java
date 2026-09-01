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
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.CanonicalSnapshotHeader;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.Hash256;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.ReservationMetadata;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CanonicalSnapshotHeaderTest {

    private static final class TestEndpoint implements RaftEndpoint {
        private final UUID uuid;

        TestEndpoint(UUID uuid) {
            this.uuid = uuid;
        }

        @Override
        public UUID getUuid() {
            return uuid;
        }
    }

    @Test
    public void testAuthoritativeMemberSortingAndDeterministicDigest() {
        CPGroupId groupId = new RaftGroupId("test-group", 12345L, 67890L);
        UUID u1 = UUID.fromString("00000000-0000-0000-0000-000000000001");
        UUID u2 = UUID.fromString("00000000-0000-0000-0000-000000000002");

        RaftEndpoint m1 = new TestEndpoint(u1);
        RaftEndpoint m2 = new TestEndpoint(u2);

        CanonicalSnapshotHeader h1 = new CanonicalSnapshotHeader(
            groupId, 1L, 100L, 2, 50L,
            List.of(m2, m1),
            Map.of((short) 2, (short) 1, (short) 1, (short) 1),
            10L, 20L, 4096L, 10L,
            List.of(new ReservationMetadata(11L, 10, 100)),
            List.of(new Hash256(new byte[32]))
        );

        CanonicalSnapshotHeader h2 = new CanonicalSnapshotHeader(
            groupId, 1L, 100L, 2, 50L,
            List.of(m1, m2),
            Map.of((short) 1, (short) 1, (short) 2, (short) 1),
            10L, 20L, 4096L, 10L,
            List.of(new ReservationMetadata(11L, 10, 100)),
            List.of(new Hash256(new byte[32]))
        );

        assertEquals("Constructor must enforce canonical sorting", h1.orderedMembers().get(0).getUuid(), u1);
        assertEquals(h1.orderedMembers(), h2.orderedMembers());

        Hash256 digest1 = h1.computeRepresentationDigest();
        Hash256 digest2 = h2.computeRepresentationDigest();

        assertNotNull(digest1);
        assertEquals("Canonical representation digest must be deterministic regardless of input order", digest1, digest2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNonAscendingReservationsRejected() {
        CPGroupId groupId = new RaftGroupId("test-group", 1L, 1L);
        RaftEndpoint m = new TestEndpoint(UUID.randomUUID());

        new CanonicalSnapshotHeader(
            groupId, 1L, 100L, 1, 50L,
            List.of(m),
            Map.of((short) 1, (short) 1),
            1L, 2L, 100L, 1L,
            List.of(new ReservationMetadata(5L, 10, 100), new ReservationMetadata(3L, 10, 100)),
            List.of()
        );
    }
}
