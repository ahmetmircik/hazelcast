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

import com.hazelcast.cp.internal.raft.impl.RaftDataSerializerConstants;
import com.hazelcast.cp.internal.raft.impl.RaftDataSerializerHook;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotChunk;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotEntry;
import com.hazelcast.cp.internal.raft.impl.testing.TestRaftEndpoint;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
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
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Verifies the file-backed {@link FileRaftStateStore}: persist + reload
 * round-trip for metadata, log entries and snapshot chunks; truncation;
 * superseding and deletion of published snapshots; and torn-tail tolerance
 * (crash-safety of the append-only log).
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class FileRaftStateStoreTest {

    private Path dir;
    private SerializationService serializationService;

    @Before
    public void before() throws IOException {
        dir = Files.createTempDirectory("raft-state-store-test");
        serializationService = new DefaultSerializationServiceBuilder()
                .addDataSerializableFactory(RaftDataSerializerConstants.F_ID,
                        new RaftDataSerializerHook().createFactory())
                .build();
    }

    @After
    public void after() throws IOException {
        deleteRecursively(dir);
    }

    @Test
    public void persistAndReload_roundTripsAllState() throws IOException {
        RaftEndpoint local = endpoint();
        Collection<RaftEndpoint> members = List.of(local, endpoint(), endpoint());

        FileRaftStateStore store = new FileRaftStateStore(dir, serializationService);
        store.open();
        store.persistInitialMembers(local, members);
        store.persistTerm(7, members.iterator().next());
        for (long i = 1; i <= 5; i++) {
            store.persistEntry(new LogEntry(7, i, "op-" + i));
        }
        store.flushLogs();
        store.close();

        RestoredRaftState restored = reopenAndLoad();
        assertEquals(local, restored.localEndpoint());
        assertEquals(members, restored.initialMembers());
        assertEquals(7, restored.term());
        assertEquals(members.iterator().next(), restored.votedFor());
        assertEquals(5, restored.entries().length);
        for (long i = 1; i <= 5; i++) {
            assertEquals(new LogEntry(7, i, "op-" + i), restored.entries()[(int) i - 1]);
        }
        assertNull(restored.snapshot());
    }

    @Test
    public void deleteEntriesFrom_truncatesTheTail() throws IOException {
        FileRaftStateStore store = openWithEntries(5);

        store.deleteEntriesFrom(3);
        store.flushLogs();
        store.close();

        RestoredRaftState restored = reopenAndLoad();
        assertEquals(2, restored.entries().length);
        assertEquals(1L, restored.entries()[0].index());
        assertEquals(2L, restored.entries()[1].index());
    }

    @Test
    public void snapshotChunks_arePublishedAtomically_onFlush() throws IOException {
        RaftEndpoint local = endpoint();
        List<RaftEndpoint> members = List.of(local, endpoint());

        FileRaftStateStore store = new FileRaftStateStore(dir, serializationService);
        store.open();
        store.persistInitialMembers(local, members);
        store.persistSnapshotChunk(chunk(1, 0, 2, "part-0", members));
        store.persistSnapshotChunk(chunk(1, 1, 2, "part-1", members));

        // before flush: staging exists, nothing published
        assertTrue(Files.exists(dir.resolve("staging-1")));
        assertTrue(!Files.exists(dir.resolve("snapshot-1")));

        store.flushLogs();
        store.close();

        // after flush: staged chunks atomically renamed into snapshot-1
        assertTrue(!Files.exists(dir.resolve("staging-1")));
        assertTrue(Files.exists(dir.resolve("snapshot-1")));

        RestoredRaftState restored = reopenAndLoad();
        SnapshotEntry snapshot = restored.snapshot();
        assertNotNull(snapshot);
        assertEquals(1L, snapshot.index());
        assertEquals(1, snapshot.term());
        assertEquals(Set.copyOf(members), Set.copyOf(snapshot.groupMembers()));
        List<SnapshotChunk> chunks = (List<SnapshotChunk>) snapshot.operation();
        assertEquals(2, chunks.size());
        assertEquals("part-0", chunks.get(0).operation());
        assertEquals("part-1", chunks.get(1).operation());
        // entries at/below the snapshot index are not reloaded
        assertEquals(0, restored.entries().length);
    }

    @Test
    public void newerSnapshot_supersedesOlderOne() throws IOException {
        RaftEndpoint local = endpoint();
        List<RaftEndpoint> members = List.of(local);

        FileRaftStateStore store = new FileRaftStateStore(dir, serializationService);
        store.open();
        store.persistInitialMembers(local, members);
        store.persistSnapshotChunk(chunk(1, 0, 1, "v1", members));
        store.flushLogs();
        store.persistSnapshotChunk(chunk(2, 0, 1, "v2", members));
        store.flushLogs();
        store.close();

        RestoredRaftState restored = reopenAndLoad();
        assertEquals(2L, restored.snapshot().index());
        assertEquals("v2", ((List<SnapshotChunk>) restored.snapshot().operation()).get(0).operation());
    }

    @Test
    public void deleteSnapshotChunks_removesPublishedSnapshot() throws IOException {
        RaftEndpoint local = endpoint();
        List<RaftEndpoint> members = List.of(local);

        FileRaftStateStore store = new FileRaftStateStore(dir, serializationService);
        store.open();
        store.persistInitialMembers(local, members);
        store.persistSnapshotChunk(chunk(1, 0, 1, "v1", members));
        store.flushLogs();
        store.deleteSnapshotChunks(1);
        store.close();

        RestoredRaftState restored = reopenAndLoad();
        assertNull(restored.snapshot());
        assertTrue(!Files.exists(dir.resolve("snapshot-1")));
    }

    @Test
    public void tornTailRecord_isToleratedOnReload() throws IOException {
        FileRaftStateStore store = openWithEntries(5);
        store.flushLogs();
        store.close();

        // simulate a crash mid-write: append garbage partial record
        byte[] garbage = new byte[]{1, 2, 3, 4, 5};
        Files.write(dir.resolve("raft.log"), garbage,
                java.nio.file.StandardOpenOption.APPEND);

        RestoredRaftState restored = reopenAndLoad();
        assertEquals(5, restored.entries().length);
        assertEquals(new LogEntry(1, 1, "op-1"), restored.entries()[0]);
    }

    @Test
    public void loadWithoutPersistedState_fails() throws IOException {
        FileRaftStateStore store = new FileRaftStateStore(dir, serializationService);
        store.open();
        assertThrows(IOException.class, store::load);
        store.close();
    }

    private FileRaftStateStore openWithEntries(int count) throws IOException {
        RaftEndpoint local = endpoint();
        FileRaftStateStore store = new FileRaftStateStore(dir, serializationService);
        store.open();
        store.persistInitialMembers(local, List.of(local));
        for (long i = 1; i <= count; i++) {
            store.persistEntry(new LogEntry(1, i, "op-" + i));
        }
        return store;
    }

    private RestoredRaftState reopenAndLoad() throws IOException {
        FileRaftStateStore reopened = new FileRaftStateStore(dir, serializationService);
        reopened.open();
        RestoredRaftState state = reopened.load();
        reopened.close();
        return state;
    }

    private static SnapshotChunk chunk(int term, int chunkNumber, int chunkCount,
                                       String payload, List<RaftEndpoint> members) {
        SnapshotChunk chunk = new SnapshotChunk()
                .setChunkCount(chunkCount)
                .setChunkNumber(chunkNumber)
                .setGroupMembers(members)
                .setGroupMembersLogIndex(term - 1);
        // LogEntry setters return LogEntry, so they cannot be chained onto
        // the SnapshotChunk expression above.
        chunk.setIndex(term); // snapshot index == term in these tests
        chunk.setOperation(payload);
        chunk.setSnapshotTerm(term);
        return chunk;
    }

    private static RaftEndpoint endpoint() {
        return new TestRaftEndpoint(UUID.randomUUID(), 5701);
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
