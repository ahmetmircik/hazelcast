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

package com.hazelcast.cp.internal.raft.impl;

import com.hazelcast.cp.internal.datastructures.cpmap.CPMapServiceUtil;
import com.hazelcast.cp.internal.datastructures.cpmap.store.CPMapStore;
import com.hazelcast.cp.internal.datastructures.cpmap.store.DeltaEmittingCPMapStore;
import com.hazelcast.cp.internal.datastructures.cpmap.store.HeapCPMapStore;
import com.hazelcast.cp.internal.datastructures.snapshot.DeltaContext;
import com.hazelcast.cp.internal.datastructures.snapshot.DeltaRecord;
import com.hazelcast.cp.internal.raft.impl.persistence.FileIncrementalSnapshotStore;
import com.hazelcast.cp.internal.raft.impl.persistence.IncrementalSnapshotStore;
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
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * End-to-end wiring test of the snapshot pipeline (Option F): a Raft-thread
 * apply (DeltaContext) feeding a {@link DeltaEmittingCPMapStore} decorator,
 * draining into a real {@link FileIncrementalSnapshotStore} on a worker, with
 * completion/failure callbacks — proving the Domain 1/Domain 2 boundary and
 * the "durable S advances off the Raft thread" claim.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SnapshotPipelineTest {

    private Path dir;
    private AtomicInteger completions;
    private AtomicInteger failures;

    @Before
    public void before() throws IOException {
        dir = Files.createTempDirectory("snapshot-pipeline-test");
        completions = new AtomicInteger();
        failures = new AtomicInteger();
    }

    @After
    public void after() throws IOException {
        deleteRecursively(dir);
    }

    @Test
    public void applyEmits_workerPersists_durableSAdvances() throws Exception {
        try (FileIncrementalSnapshotStore store = new FileIncrementalSnapshotStore(dir);
             SnapshotPipeline pipeline = new SnapshotPipeline(store, inlineExecutor(),
                     completions::incrementAndGet, failures::incrementAndGet, 1)) {

            CPMapStore emittingStore = new DeltaEmittingCPMapStore(CPMapServiceUtil.SERVICE_NAME, "map",
                    new HeapCPMapStore(1));
            Data key = data();
            Data value = data();

            // Raft thread apply, index 42
            try (DeltaContext ignored = pipeline.beginApply(42)) {
                emittingStore.put(key, value, 7L);
            }
            pipeline.onApplied();

            assertEquals(1, completions.get());
            assertEquals(0, failures.get());
            assertEquals(42L, pipeline.durableSnapshotIndex());
            assertEquals(0, pipeline.pendingDeltaCount());
            assertTrue(!pipeline.isDrainInFlight());

            List<DeltaRecord> deltas = store.readAllDeltas();
            assertEquals(1, deltas.size());
            assertEquals(42L, deltas.get(0).opIndex());
            // the on-disk round trip reconstructs HeapData, so compare content
            assertArrayEquals(key.toByteArray(), deltas.get(0).key().toByteArray());
            assertArrayEquals(value.toByteArray(), deltas.get(0).value().toByteArray());
        }
    }

    @Test
    public void drainFailure_requeuesDeltas_andNotifies() throws Exception {
        IncrementalSnapshotStore failingStore = new IncrementalSnapshotStore() {
            @Override
            public void appendDeltas(List<DeltaRecord> deltas, long batchIndex) throws IOException {
                throw new IOException("disk full");
            }

            @Override
            public void flush() {
            }

            @Override
            public long durableSnapshotIndex() {
                return 0;
            }

            @Override
            public void close() {
            }

            @Override
            public void delete() {
            }
        };

        try (SnapshotPipeline pipeline = new SnapshotPipeline(failingStore, inlineExecutor(),
                completions::incrementAndGet, failures::incrementAndGet, 1)) {

            try (DeltaContext ignored = pipeline.beginApply(5)) {
                new DeltaEmittingCPMapStore(CPMapServiceUtil.SERVICE_NAME, "map",
                        new HeapCPMapStore(1)).put(data(), data(), 1L);
            }
            pipeline.onApplied();

            assertEquals(0, completions.get());
            assertEquals(1, failures.get());
            // the failed deltas were re-queued, not lost
            assertEquals(1, pipeline.pendingDeltaCount());
        }
    }

    @Test
    public void atMostOneDrainInFlight_coalesces() throws Exception {
        AtomicInteger submitted = new AtomicInteger();
        Executor blocking = task -> {
            submitted.incrementAndGet();
            // never run: the first drain stays in flight
        };

        try (FileIncrementalSnapshotStore store = new FileIncrementalSnapshotStore(dir);
             SnapshotPipeline pipeline = new SnapshotPipeline(store, blocking,
                     completions::incrementAndGet, failures::incrementAndGet, 1)) {

            apply(pipeline, 1);
            apply(pipeline, 2);
            apply(pipeline, 3);

            assertEquals(1, submitted.get()); // coalesced: one drain in flight
            assertTrue(pipeline.isDrainInFlight());
            assertEquals(3, pipeline.pendingDeltaCount()); // nothing lost
        }
    }

    private static void apply(SnapshotPipeline pipeline, long opIndex) {
        try (DeltaContext ignored = pipeline.beginApply(opIndex)) {
            new DeltaEmittingCPMapStore(CPMapServiceUtil.SERVICE_NAME, "map",
                    new HeapCPMapStore(1)).put(data(), data(), 1L);
        }
        pipeline.onApplied();
    }

    private static Data data() {
        Data data = mock(Data.class);
        when(data.toByteArray()).thenReturn(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12});
        return data;
    }

    private static Executor inlineExecutor() {
        return Runnable::run;
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
