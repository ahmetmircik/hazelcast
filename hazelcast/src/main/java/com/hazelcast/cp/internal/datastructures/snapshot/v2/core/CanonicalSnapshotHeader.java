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

package com.hazelcast.cp.internal.datastructures.snapshot.v2.core;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

/**
 * Canonical deterministic snapshot header with defensive copy constructor.
 */
public record CanonicalSnapshotHeader(
    CPGroupId groupId,
    long groupIncarnation,
    long snapshotIndex,
    int snapshotTerm,
    long membershipLogIndex,
    List<RaftEndpoint> orderedMembers,
    Map<Short, Short> codecDirectory,
    long firstDeltaSequence,
    long lastDeltaSequence,
    long declaredDataBytes,
    long lastQuorumAppliedReservationSeq,
    List<ReservationMetadata> unreleasedReservationsAtCut,
    List<Hash256> framePayloadChecksums
) implements Serializable {

    public static final long HZ_SNAP1_MAGIC = 0x485A5F534E415031L;
    public static final int MAX_COLLECTION_COUNT = 65535;
    public static final int MAX_RESERVATIONS_COUNT = 64;
    public static final int MAX_STRING_LENGTH = 256;
    public static final int MAX_ENTRIES_PER_PROPOSAL = 256;
    public static final int MAX_BYTES_PER_PROPOSAL = 4096;

    private static final int BUFFER_CAPACITY = 1024;

    public CanonicalSnapshotHeader {
        validateScalars(groupId, groupIncarnation, snapshotIndex, snapshotTerm);
        validateCollections(orderedMembers, codecDirectory, unreleasedReservationsAtCut);

        List<RaftEndpoint> sortedMembers = new ArrayList<>(orderedMembers != null ? orderedMembers : List.of());
        sortedMembers.sort(Comparator.comparing(RaftEndpoint::getUuid));
        orderedMembers = Collections.unmodifiableList(sortedMembers);

        codecDirectory = Collections.unmodifiableMap(new TreeMap<>(codecDirectory != null ? codecDirectory : Map.of()));
        unreleasedReservationsAtCut = Collections.unmodifiableList(
                new ArrayList<>(unreleasedReservationsAtCut != null ? unreleasedReservationsAtCut : List.of()));
        framePayloadChecksums = Collections.unmodifiableList(
                new ArrayList<>(framePayloadChecksums != null ? framePayloadChecksums : List.of()));
    }

    private static void validateScalars(CPGroupId groupId, long groupIncarnation, long snapshotIndex, int snapshotTerm) {
        if (groupId == null) {
            throw new IllegalArgumentException("groupId must not be null");
        }
        if (groupIncarnation <= 0) {
            throw new IllegalArgumentException("groupIncarnation must be positive");
        }
        if (snapshotIndex < 0) {
            throw new IllegalArgumentException("snapshotIndex must be non-negative");
        }
        if (snapshotTerm < 0) {
            throw new IllegalArgumentException("snapshotTerm must be non-negative");
        }
    }

    private static void validateCollections(
            List<RaftEndpoint> orderedMembers,
            Map<Short, Short> codecDirectory,
            List<ReservationMetadata> unreleasedReservationsAtCut) {
        if (orderedMembers != null && orderedMembers.size() > MAX_COLLECTION_COUNT) {
            throw new IllegalArgumentException("orderedMembers count exceeds max collection bound");
        }
        if (codecDirectory != null && codecDirectory.size() > MAX_COLLECTION_COUNT) {
            throw new IllegalArgumentException("codecDirectory count exceeds max collection bound");
        }
        if (unreleasedReservationsAtCut != null) {
            if (unreleasedReservationsAtCut.size() > MAX_RESERVATIONS_COUNT) {
                throw new IllegalArgumentException("unreleasedReservationsAtCut count exceeds max bound");
            }
            long lastSeq = -1L;
            for (ReservationMetadata r : unreleasedReservationsAtCut) {
                if (r.reservationSequence() <= lastSeq) {
                    throw new IllegalArgumentException("Reservation sequences must be strictly ascending");
                }
                lastSeq = r.reservationSequence();
            }
        }
    }

    public List<RaftEndpoint> members() {
        return orderedMembers;
    }

    public long snapshotFloor() {
        return lastQuorumAppliedReservationSeq;
    }

    public List<ReservationMetadata> unreleasedReservations() {
        return unreleasedReservationsAtCut;
    }

    public Hash256 representationDigest() {
        return computeRepresentationDigest();
    }

    public Hash256 computeRepresentationDigest() {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");

            ByteBuffer bb = ByteBuffer.allocate(BUFFER_CAPACITY).order(ByteOrder.BIG_ENDIAN);
            bb.putLong(HZ_SNAP1_MAGIC);

            if (groupId instanceof RaftGroupId raftGroupId) {
                byte[] nameBytes = raftGroupId.getName().getBytes(StandardCharsets.UTF_8);
                bb.putInt(nameBytes.length);
                bb.put(nameBytes);
                bb.putLong(raftGroupId.getSeed());
                bb.putLong(raftGroupId.getId());
            } else {
                byte[] nameBytes = groupId.getName().getBytes(StandardCharsets.UTF_8);
                bb.putInt(nameBytes.length);
                bb.put(nameBytes);
                bb.putLong(0L);
                bb.putLong(0L);
            }

            bb.putLong(groupIncarnation);
            bb.putLong(snapshotIndex);
            bb.putInt(snapshotTerm);
            bb.putLong(membershipLogIndex);

            bb.putInt(orderedMembers.size());
            for (RaftEndpoint endpoint : orderedMembers) {
                UUID uuid = endpoint.getUuid();
                bb.putLong(uuid.getMostSignificantBits());
                bb.putLong(uuid.getLeastSignificantBits());
            }

            bb.putInt(codecDirectory.size());
            for (Map.Entry<Short, Short> entry : codecDirectory.entrySet()) {
                bb.putShort(entry.getKey());
                bb.putShort(entry.getValue());
            }

            bb.putLong(firstDeltaSequence);
            bb.putLong(lastDeltaSequence);
            bb.putLong(declaredDataBytes);
            bb.putLong(lastQuorumAppliedReservationSeq);

            bb.putInt(unreleasedReservationsAtCut.size());
            for (ReservationMetadata r : unreleasedReservationsAtCut) {
                bb.putLong(r.reservationSequence());
                bb.putInt(r.worstCaseEntries());
                bb.putInt(r.worstCaseBytes());
            }

            bb.flip();
            md.update(bb);

            for (Hash256 chunkHash : framePayloadChecksums) {
                md.update(chunkHash.asBytes());
            }

            return new Hash256(md.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
