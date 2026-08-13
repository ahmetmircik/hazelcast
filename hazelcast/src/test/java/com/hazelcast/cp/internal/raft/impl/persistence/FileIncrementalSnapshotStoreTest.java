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
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static com.hazelcast.cp.internal.datastructures.snapshot.DeltaRecord.Kind.PUT;
import static java.util.List.of;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the delta "writing" side ({@link FileIncrementalSnapshotStore}):
 * the durable snapshot index S advances only after {@link #flush()}
 * (group commit), survives a reopen, and un-flushed tails stay replayable —
 * the crash-safety anchor of the redesign's "truncate only after durable
 * publish" guarantee.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class FileIncrementalSnapshotStoreTest {

    private Path dir;

    @Before
    public void before() throws IOException {
        dir = Files.createTempDirectory("incremental-snapshot-store-test");
    }

    @After
    public void after() throws IOException {
        deleteRecursively(dir);
    }

    @Test
    public void appendAndFlush_advancesDurableS_andSurvivesReopen() throws IOException {
        try (FileIncrementalSnapshotStore store = new FileIncrementalSnapshotStore(dir)) {
            assertEquals(0L, store.durableSnapshotIndex());
            store.appendDeltas(of(record(1)), 100L);
            assertEquals(0L, store.durableSnapshotIndex()); // not durable yet
            store.flush();
            assertEquals(100L, store.durableSnapshotIndex());
        }

        try (FileIncrementalSnapshotStore reopened = new FileIncrementalSnapshotStore(dir)) {
            assertEquals(100L, reopened.durableSnapshotIndex());
            List<DeltaRecord> deltas = reopened.readAllDeltas();
            assertEquals(1, deltas.size());
            // the on-disk round trip reconstructs HeapData, so compare content
            assertEquals(record(1).opIndex(), deltas.get(0).opIndex());
            assertEquals(record(1).kind(), deltas.get(0).kind());
            assertEquals(record(1).serviceName(), deltas.get(0).serviceName());
            assertEquals(record(1).objectName(), deltas.get(0).objectName());
            assertEquals(record(1).timestamp(), deltas.get(0).timestamp());
            assertArrayEquals(record(1).key().toByteArray(), deltas.get(0).key().toByteArray());
            assertArrayEquals(record(1).value().toByteArray(), deltas.get(0).value().toByteArray());
        }
    }

    @Test
    public void unflushedTail_isReplayableAfterReopen_butSDoesNotAdvance() throws IOException {
        FileIncrementalSnapshotStore store = new FileIncrementalSnapshotStore(dir);
        store.appendDeltas(of(record(1), record(2)), 50L);
        store.close(); // crash-simulating close without flush

        try (FileIncrementalSnapshotStore reopened = new FileIncrementalSnapshotStore(dir)) {
            // S did not advance, but the deltas are on disk and replayable
            assertEquals(0L, reopened.durableSnapshotIndex());
            assertEquals(2, reopened.readAllDeltas().size());
        }
    }

    @Test
    public void appendBatchBelowDurableS_isRejected() throws IOException {
        try (FileIncrementalSnapshotStore store = new FileIncrementalSnapshotStore(dir)) {
            store.appendDeltas(of(record(1)), 100L);
            store.flush();
            assertThrows(IllegalArgumentException.class, () -> store.appendDeltas(of(record(2)), 99L));
        }
    }

    @Test
    public void delete_removesAllPersistedState() throws IOException {
        FileIncrementalSnapshotStore store = new FileIncrementalSnapshotStore(dir);
        store.appendDeltas(of(record(1)), 100L);
        store.flush();
        store.delete();
        assertTrue(!Files.exists(dir.resolve("deltas.log")));
        assertTrue(!Files.exists(dir.resolve("durable-s.dat")));
    }

    private static DeltaRecord record(long opIndex) {
        return new DeltaRecord(PUT, "svc", "map", data(), data(), 1L, opIndex);
    }

    private static Data data() {
        Data data = mock(Data.class);
        when(data.toByteArray()).thenReturn(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12});
        return data;
    }

    private static void deleteRecursively(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        if (Files.isDirectory(path)) {
            try (var files = Files.list(path)) {
                for (Path child : files.toList()) {
                    deleteRecursively(child);
                }
            }
        }
        Files.deleteIfExists(path);
    }
}
