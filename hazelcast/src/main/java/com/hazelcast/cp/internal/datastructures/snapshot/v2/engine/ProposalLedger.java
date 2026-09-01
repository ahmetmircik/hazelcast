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

package com.hazelcast.cp.internal.datastructures.snapshot.v2.engine;

import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.LedgerRangeLease;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.ReservationMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Pre-consensus proposal admission ledger backed by a fixed-capacity circular ring.
 */
public final class ProposalLedger {

    public static final int LEDGER_CAPACITY = 256;
    public static final int P_MAX = 64;
    public static final int E_MAX = 256;
    public static final int B_MAX = 4096;

    public static final int H_VOTER_ENTRIES = P_MAX * E_MAX;
    public static final int H_VOTER_BYTES = P_MAX * B_MAX;

    private static final int LEDGER_MASK = LEDGER_CAPACITY - 1;

    private final AtomicReferenceArray<ReservationMetadata> reservationRing;
    private final AtomicLong nextSequence;
    private final AtomicLong quorumAppliedFloor;
    private final AtomicLong inFlightEntries;
    private final AtomicLong inFlightBytes;
    private final AtomicLong leaseGen;

    public ProposalLedger(long initialSequence) {
        this.reservationRing = new AtomicReferenceArray<>(LEDGER_CAPACITY);
        this.nextSequence = new AtomicLong(initialSequence);
        this.quorumAppliedFloor = new AtomicLong(initialSequence);
        this.inFlightEntries = new AtomicLong(0L);
        this.inFlightBytes = new AtomicLong(0L);
        this.leaseGen = new AtomicLong(0L);
    }

    public long inFlightEntries() {
        return inFlightEntries.get();
    }

    public long inFlightBytes() {
        return inFlightBytes.get();
    }

    public long nextSequence() {
        return nextSequence.get();
    }

    public long quorumAppliedFloor() {
        return quorumAppliedFloor.get();
    }

    public boolean canAdmit() {
        long seq = nextSequence.get();
        long floor = quorumAppliedFloor.get();
        return (seq - floor) < P_MAX;
    }

    public ReservationMetadata admit(int worstCaseEntries, int worstCaseBytes) {
        if (worstCaseEntries <= 0 || worstCaseEntries > E_MAX) {
            throw new IllegalArgumentException("worstCaseEntries must be in (0, " + E_MAX + "]");
        }
        if (worstCaseBytes <= 0 || worstCaseBytes > B_MAX) {
            throw new IllegalArgumentException("worstCaseBytes must be in (0, " + B_MAX + "]");
        }

        long floor = quorumAppliedFloor.get();
        long seq = nextSequence.get();
        if ((seq - floor) >= P_MAX) {
            throw new IllegalStateException("Proposal admission throttled: in-flight window full (P_MAX=" + P_MAX + ")");
        }

        seq = nextSequence.getAndIncrement();
        ReservationMetadata meta = new ReservationMetadata(seq, worstCaseEntries, worstCaseBytes);
        int slot = (int) (seq & LEDGER_MASK);

        reservationRing.lazySet(slot, meta);
        inFlightEntries.updateAndGet(curr -> Math.addExact(curr, worstCaseEntries));
        inFlightBytes.updateAndGet(curr -> Math.addExact(curr, worstCaseBytes));

        return meta;
    }

    public void acknowledgeQuorumApplied(long acknowledgedSeq) {
        long floor = quorumAppliedFloor.get();
        if (acknowledgedSeq <= floor) {
            return;
        }

        for (long s = floor; s < acknowledgedSeq; s++) {
            int slot = (int) (s & LEDGER_MASK);
            ReservationMetadata meta = reservationRing.get(slot);
            if (meta != null && meta.reservationSequence() == s) {
                inFlightEntries.updateAndGet(curr -> Math.max(0L, curr - meta.worstCaseEntries()));
                inFlightBytes.updateAndGet(curr -> Math.max(0L, curr - meta.worstCaseBytes()));
            }
        }
        quorumAppliedFloor.set(acknowledgedSeq);
    }

    public LedgerRangeLease captureRangeLease() {
        return new LedgerRangeLease(quorumAppliedFloor.get(), nextSequence.get() - 1, leaseGen.incrementAndGet());
    }

    public List<ReservationMetadata> extractAscendingReservations(LedgerRangeLease lease) {
        long start = lease.snapshotFloor();
        long end = lease.lastAdmittedSequence();
        if (start > end) {
            return List.of();
        }

        List<ReservationMetadata> result = new ArrayList<>();
        for (long s = start; s <= end; s++) {
            int slot = (int) (s & LEDGER_MASK);
            ReservationMetadata meta = reservationRing.get(slot);
            if (meta != null && meta.reservationSequence() == s) {
                result.add(meta);
            }
        }
        return List.copyOf(result);
    }

    public void reconstruct(long snapshotFloor, List<ReservationMetadata> restoredReservations) {
        long floor = Math.max(snapshotFloor, quorumAppliedFloor.get());
        quorumAppliedFloor.set(floor);
        inFlightEntries.set(0L);
        inFlightBytes.set(0L);

        long maxSeq = floor;
        for (ReservationMetadata r : restoredReservations) {
            if (r.reservationSequence() >= floor) {
                int slot = (int) (r.reservationSequence() & LEDGER_MASK);
                reservationRing.lazySet(slot, r);
                inFlightEntries.updateAndGet(curr -> Math.addExact(curr, r.worstCaseEntries()));
                inFlightBytes.updateAndGet(curr -> Math.addExact(curr, r.worstCaseBytes()));
                maxSeq = Math.max(maxSeq, r.reservationSequence() + 1);
            }
        }
        nextSequence.set(maxSeq);
    }
}
