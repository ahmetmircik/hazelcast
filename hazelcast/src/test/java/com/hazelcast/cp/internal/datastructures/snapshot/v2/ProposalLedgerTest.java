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

import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.LedgerRangeLease;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.ReservationMetadata;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.engine.ProposalLedger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ProposalLedgerTest {

    @Test
    public void testAdmissionAndQuorumRetirement() {
        ProposalLedger ledger = new ProposalLedger(1L);

        ReservationMetadata r1 = ledger.admit(10, 100);
        assertEquals(1L, r1.reservationSequence());
        assertEquals(10L, ledger.inFlightEntries());
        assertEquals(100L, ledger.inFlightBytes());

        ReservationMetadata r2 = ledger.admit(20, 200);
        assertEquals(2L, r2.reservationSequence());
        assertEquals(30L, ledger.inFlightEntries());
        assertEquals(300L, ledger.inFlightBytes());

        LedgerRangeLease lease = ledger.captureRangeLease();
        List<ReservationMetadata> extracted = ledger.extractAscendingReservations(lease);
        assertEquals(2, extracted.size());

        ledger.acknowledgeQuorumApplied(2L);
        assertEquals(20L, ledger.inFlightEntries());
        assertEquals(200L, ledger.inFlightBytes());

        ledger.acknowledgeQuorumApplied(3L);
        assertEquals(0L, ledger.inFlightEntries());
        assertEquals(0L, ledger.inFlightBytes());
    }

    @Test
    public void testReconstructionAndThrottling() {
        ProposalLedger ledger = new ProposalLedger(10L);

        ReservationMetadata r1 = new ReservationMetadata(10L, 5, 50);
        ReservationMetadata r2 = new ReservationMetadata(11L, 10, 100);

        ledger.reconstruct(10L, List.of(r1, r2));
        assertEquals(15L, ledger.inFlightEntries());
        assertEquals(150L, ledger.inFlightBytes());
        assertEquals(12L, ledger.nextSequence());

        // Fill window up to P_MAX
        for (int i = 0; i < ProposalLedger.P_MAX - 2; i++) {
            assertTrue(ledger.canAdmit());
            ledger.admit(1, 10);
        }

        assertFalse("Window must be full after P_MAX unacknowledged proposals", ledger.canAdmit());
    }
}
