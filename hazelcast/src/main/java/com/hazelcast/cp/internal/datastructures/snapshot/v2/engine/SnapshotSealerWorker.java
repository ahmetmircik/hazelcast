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

import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.CPServiceSnapshotCodec;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.CanonicalSnapshotHeader;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.DeltaEntry;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.FrozenDeltaSegment;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.Hash256;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.LedgerRangeLease;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.ReservationMetadata;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.ServiceBaseRef;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.SnapshotFrameSink;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Off-thread snapshot sealer worker with bounded queue (1 in-flight + 1 coalesced pending slot).
 */
public final class SnapshotSealerWorker implements Runnable {

    public record SealerTask(
        SnapshotRepresentation candidate,
        LedgerRangeLease ledgerLease,
        ProposalLedger proposalLedger,
        int snapshotTerm,
        long membershipLogIndex,
        List<RaftEndpoint> orderedMembers,
        Map<Short, CPServiceSnapshotCodec<?, ?>> codecRegistry,
        Consumer<SnapshotRepresentation> publicationCallback
    ) {}

    private final BlockingQueue<Boolean> signalQueue = new LinkedBlockingQueue<>(1);
    private final AtomicReference<SealerTask> pendingSlot = new AtomicReference<>(null);
    private volatile boolean running = true;

    public boolean offerTask(SealerTask task) {
        SealerTask prev = pendingSlot.getAndSet(task);
        if (prev != null) {
            prev.candidate().markSealFailed();
        }
        signalQueue.offer(Boolean.TRUE);
        return true;
    }

    public void stop() {
        running = false;
        signalQueue.offer(Boolean.TRUE);
    }

    public void shutdown() {
        stop();
    }


    @Override
    public void run() {
        while (running) {
            try {
                signalQueue.take();
                SealerTask task = pendingSlot.getAndSet(null);
                if (task != null) {
                    processSealing(task);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Throwable t) {
                // Keep worker resilient
            }
        }
    }

    public void processSealing(SealerTask task) {
        SnapshotRepresentation candidate = task.candidate();
        try {
            List<ReservationMetadata> unreleased = task.proposalLedger()
                    .extractAscendingReservations(task.ledgerLease());

            FrameHashingSink sink = new FrameHashingSink();
            serializePayloadFrames(candidate, task.codecRegistry(), sink);

            long firstSeq = candidate.leasedDeltas().isEmpty() ? 0 : candidate.leasedDeltas().get(0).sequence();
            long lastSeq = candidate.leasedDeltas().isEmpty()
                    ? 0 : candidate.leasedDeltas().get(candidate.leasedDeltas().size() - 1).sequence();

            Map<Short, Short> codecDirectory = new java.util.TreeMap<>();
            for (var entry : task.codecRegistry().entrySet()) {
                codecDirectory.put(entry.getKey(), entry.getValue().codecVersion());
            }

            CanonicalSnapshotHeader header = new CanonicalSnapshotHeader(
                candidate.id().groupId(),
                candidate.id().groupIncarnation(),
                candidate.id().snapshotIndex(),
                task.snapshotTerm(),
                task.membershipLogIndex(),
                task.orderedMembers(),
                codecDirectory,
                firstSeq,
                lastSeq,
                sink.totalBytesWritten(),
                task.ledgerLease().snapshotFloor(),
                unreleased,
                sink.chunkHashes()
            );

            boolean sealed = candidate.seal(header);
            if (sealed) {
                task.publicationCallback().accept(candidate);
            }
        } catch (Throwable t) {
            candidate.markSealFailed();
        }
    }

    @SuppressWarnings("unchecked")
    private void serializePayloadFrames(SnapshotRepresentation candidate,
                                        Map<Short, CPServiceSnapshotCodec<?, ?>> codecs,
                                        SnapshotFrameSink sink) throws IOException {
        for (Map.Entry<Short, ServiceBaseRef> entry : candidate.baseRef().serviceBases().entrySet()) {
            CPServiceSnapshotCodec codec = codecs.get(entry.getKey());
            if (codec != null) {
                codec.writeBase(entry.getValue().immutableServicePayload(), sink);
            }
        }

        for (FrozenDeltaSegment seg : candidate.leasedDeltas()) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            dos.writeLong(seg.sequence());
            dos.writeInt(seg.entryCount());
            Iterator<DeltaEntry> it = seg.iterator();
            while (it.hasNext()) {
                DeltaEntry d = it.next();
                dos.writeShort(d.serviceId());
                dos.writeByte(d.opCode());
                dos.writeLong(d.raftIndex());
                dos.writeInt(d.raftOrdinal());
                if (d.key() != null) {
                    byte[] k = d.key().toByteArray();
                    dos.writeInt(k.length);
                    dos.write(k);
                } else {
                    dos.writeInt(0);
                }
                if (d.value() != null) {
                    byte[] v = d.value().toByteArray();
                    dos.writeInt(v.length);
                    dos.write(v);
                } else {
                    dos.writeInt(0);
                }
            }
            dos.flush();
            byte[] segBytes = baos.toByteArray();
            int off = 0;
            while (off < segBytes.length) {
                int len = Math.min(segBytes.length - off, SnapshotFrameSink.CHUNK_SIZE_BYTES);
                sink.writeChunk(segBytes, off, len);
                off += len;
            }
        }

        if (sink.totalBytesWritten() == 0) {
            sink.writeChunk(new byte[1], 0, 1);
        }
    }

    private static final class FrameHashingSink implements SnapshotFrameSink {
        private final List<Hash256> hashes = new ArrayList<>();
        private final MessageDigest digest;
        private long total;

        FrameHashingSink() {
            try {
                this.digest = MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void writeChunk(byte[] buffer, int offset, int length) throws IOException {
            digest.reset();
            digest.update(buffer, offset, length);
            hashes.add(new Hash256(digest.digest()));
            total += length;
        }

        @Override
        public long totalBytesWritten() {
            return total;
        }

        public List<Hash256> chunkHashes() {
            return List.copyOf(hashes);
        }
    }
}
