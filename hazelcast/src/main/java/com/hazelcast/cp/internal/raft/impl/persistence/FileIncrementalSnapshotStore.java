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

package com.hazelcast.cp.internal.raft.impl.persistence;

import com.hazelcast.cp.internal.datastructures.snapshot.DeltaRecord;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

/**
 * A file-backed {@link IncrementalSnapshotStore} implementing the delta
 * "writing" side of the redesign: append-only delta log + durable snapshot
 * index {@code S}.
 * <p>
 * Layout:
 * <pre>
 *   &lt;dir&gt;/deltas.log    append-only, length-prefixed {@link DeltaRecord}s
 *   &lt;dir&gt;/durable-s.dat the durable snapshot index S (atomic write)
 * </pre>
 * Group commit: {@link #flush()} first fsyncs {@code deltas.log}, then
 * atomically rewrites {@code durable-s.dat} (temp + rename + dir fsync) with
 * the last batch index. A crash between the two leaves S at the previous
 * value and the uncommitted tail in {@code deltas.log} — on restart the
 * platform re-applies entries above S (the tail is re-playable and harmless;
 * {@link #readAllDeltas()} exposes it for compaction/rebuild).
 * <p>
 * A torn trailing record in {@code deltas.log} (crash mid-write) is silently
 * dropped on read.
 */
public class FileIncrementalSnapshotStore implements IncrementalSnapshotStore {

    private static final String DELTAS_FILE = "deltas.log";
    private static final String DURABLE_S_FILE = "durable-s.dat";
    private static final String TMP_SUFFIX = ".tmp";

    private final Path dir;

    private FileOutputStream deltasFileOut;
    private DataOutputStream deltasOut;
    private FileChannel deltasChannel;

    private long durableS;
    private long pendingBatchIndex = -1L;

    public FileIncrementalSnapshotStore(Path directory) throws IOException {
        this.dir = directory;
        Files.createDirectories(dir);
        this.durableS = readDurableS();
        deltasFileOut = new FileOutputStream(dir.resolve(DELTAS_FILE).toFile(), true);
        deltasOut = new DataOutputStream(new BufferedOutputStream(deltasFileOut));
        deltasChannel = deltasFileOut.getChannel();
    }

    @Override
    public synchronized void appendDeltas(List<DeltaRecord> deltas, long batchIndex) throws IOException {
        if (batchIndex < durableS) {
            throw new IllegalArgumentException("batchIndex=" + batchIndex + " < durableS=" + durableS);
        }
        for (DeltaRecord delta : deltas) {
            byte[] record = toBytes(delta);
            deltasOut.writeInt(record.length);
            deltasOut.write(record);
        }
        pendingBatchIndex = batchIndex;
    }

    @Override
    public synchronized void flush() throws IOException {
        deltasOut.flush();
        deltasChannel.force(true);
        if (pendingBatchIndex >= 0) {
            durableS = pendingBatchIndex;
            atomicWriteLong(dir.resolve(DURABLE_S_FILE), durableS);
        }
    }

    @Override
    public synchronized long durableSnapshotIndex() {
        return durableS;
    }

    @Override
    public synchronized void close() throws IOException {
        if (deltasOut != null) {
            deltasOut.flush();
            deltasOut.close();
            deltasOut = null;
            deltasFileOut = null;
            deltasChannel = null;
        }
    }

    @Override
    public synchronized void delete() throws IOException {
        close();
        Files.deleteIfExists(dir.resolve(DELTAS_FILE));
        Files.deleteIfExists(dir.resolve(DURABLE_S_FILE));
    }

    /**
     * Reads every delta record persisted so far (including any records above
     * the durable S, which are re-playable after a crash). Exposed for the
     * platform's rebuild/compaction path and tests.
     */
    public synchronized List<DeltaRecord> readAllDeltas() throws IOException {
        Path file = dir.resolve(DELTAS_FILE);
        if (!Files.exists(file)) {
            return List.of();
        }
        List<DeltaRecord> deltas = new ArrayList<>();
        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(new FileInputStream(file.toFile())))) {
            while (true) {
                int length;
                try {
                    length = in.readInt();
                } catch (EOFException e) {
                    break;
                }
                if (length < 0 || length > MAX_RECORD_LENGTH) {
                    break;
                }
                byte[] bytes = new byte[length];
                in.readFully(bytes);
                deltas.add(fromBytes(bytes));
            }
        } catch (EOFException e) {
            // torn tail after a crash: drop the partial record
        }
        return deltas;
    }

    private long readDurableS() throws IOException {
        Path file = dir.resolve(DURABLE_S_FILE);
        if (!Files.exists(file)) {
            return 0L;
        }
        try (DataInputStream in = new DataInputStream(new FileInputStream(file.toFile()))) {
            return in.readLong();
        }
    }

    private void atomicWriteLong(Path target, long value) throws IOException {
        Path tmp = target.resolveSibling(target.getFileName() + TMP_SUFFIX);
        try (DataOutputStream out = new DataOutputStream(
                new BufferedOutputStream(new FileOutputStream(tmp.toFile())))) {
            out.writeLong(value);
        }
        forceFile(tmp);
        Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        forceFile(target.getParent());
    }

    private static void forceFile(Path path) throws IOException {
        if (Files.isDirectory(path)) {
            try (FileChannel channel = FileChannel.open(path)) {
                channel.force(true);
            } catch (IOException ignored) {
                // directory fsync is best-effort
            }
        } else {
            try (FileChannel channel = FileChannel.open(path)) {
                channel.force(true);
            }
        }
    }

    private static byte[] toBytes(DeltaRecord delta) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            delta.writeTo(out);
        }
        return bytes.toByteArray();
    }

    private static DeltaRecord fromBytes(byte[] bytes) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            return DeltaRecord.readFrom(in);
        }
    }

    private static final int MAX_RECORD_LENGTH = 1 << 29;
}
