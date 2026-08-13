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

package com.hazelcast.cp.internal.datastructures.snapshot;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Verifies the transient write-redirection shadow protocol (Option F, design
 * tests F3/F4/F7/F8): O(1) capture, point-in-time copy, the single-phase
 * flip (no lost updates across the flip boundary), concurrent-drain safety,
 * tombstone semantics, and the deactivate-only-when-empty boundary.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SnapshotShadowTest {

    private final SnapshotShadow<String, String> shadow = new SnapshotShadow<>();

    @Test
    public void capture_redirectsWrites_andCopyIsPointInTime() {
        shadow.put("k", "v1");

        assertTrue(shadow.capture());
        assertFalse(shadow.capture()); // coalesced while active

        // writes after capture go to the overlay; main stays frozen
        shadow.put("k", "v2");
        shadow.put("k2", "v2");

        // worker copy sees exactly the state at the capture point
        List<Map.Entry<String, String>> copy = shadow.copyMain();
        assertEquals(1, copy.size());
        assertEquals("v1", valueOf(copy, "k"));

        // live reads see the latest state (overlay first)
        assertEquals("v2", shadow.get("k"));
        assertEquals("v2", shadow.get("k2"));

        // drain + flip: overlay contents land in main
        List<String> drained = new ArrayList<>();
        shadow.drainOverlay((key, value, tombstone) -> drained.add(key));
        assertTrue(shadow.tryDeactivate());
        assertFalse(shadow.isShadowActive());

        assertEquals("v2", shadow.get("k"));
        assertEquals("v2", shadow.get("k2"));
        // drain order is ConcurrentHashMap iteration order — assert the set
        assertEquals(List.of("k", "k2"), drainedSorted(drained));
    }

    private static List<String> drainedSorted(List<String> drained) {
        List<String> sorted = new ArrayList<>(drained);
        java.util.Collections.sort(sorted);
        return sorted;
    }

    @Test
    public void postFlipWrite_isNotOverwrittenByDrain() {
        // design test F8: v1 -> (capture) -> v2 (overlay) -> drain -> flip -> v3 (main)
        shadow.put("k", "v1");
        shadow.capture();
        shadow.put("k", "v2");

        shadow.drainOverlay((key, value, tombstone) -> { });
        assertTrue(shadow.tryDeactivate());

        shadow.put("k", "v3");

        assertEquals("v3", shadow.get("k"));
    }

    @Test
    public void drainWithConcurrentPut_keepsNewestValue() throws Exception {
        shadow.put("k", "v1");
        shadow.capture();
        shadow.put("k", "v2");

        CountDownLatch enteredConsumer = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Thread drainer = new Thread(() -> shadow.drainOverlay((key, value, tombstone) -> {
            enteredConsumer.countDown();
            await(release);
        }));
        drainer.start();

        // drainer is now mid-drain on (k, v2); a concurrent write lands
        assertTrue(enteredConsumer.await(5, TimeUnit.SECONDS));
        shadow.put("k", "v3");
        release.countDown();
        drainer.join(TimeUnit.SECONDS.toMillis(5));
        assertFalse(drainer.isAlive());

        assertTrue(shadow.tryDeactivate());
        assertEquals("v3", shadow.get("k"));
    }

    @Test
    public void remove_whileShadowed_isVisibleImmediately_andAppliedOnDrain() {
        shadow.put("k", "v1");
        shadow.capture();

        // removal during the shadow: reads see absent right away
        shadow.remove("k");
        assertNull(shadow.get("k"));

        // and the copy still contains the pre-removal state
        List<Map.Entry<String, String>> copy = shadow.copyMain();
        assertEquals("v1", valueOf(copy, "k"));

        shadow.drainOverlay((key, value, tombstone) -> { });
        assertTrue(shadow.tryDeactivate());

        assertNull(shadow.get("k"));
    }

    @Test
    public void tryDeactivate_onlyWhenOverlayEmpty() {
        shadow.put("k", "v1");
        shadow.capture();
        shadow.put("k2", "v2"); // overlay non-empty

        assertFalse(shadow.tryDeactivate());
        assertTrue(shadow.isShadowActive());

        shadow.drainOverlay((key, value, tombstone) -> { });
        assertTrue(shadow.tryDeactivate());
        assertFalse(shadow.isShadowActive());
        assertTrue(shadow.tryDeactivate()); // idempotent when already inactive
    }

    @Test
    public void readAndWrite_whenInactive_behaveLikePlainMap() {
        assertNull(shadow.get("k"));
        shadow.put("k", "v1");
        assertEquals("v1", shadow.get("k"));
        assertEquals("v1", shadow.remove("k"));
        assertNull(shadow.get("k"));
    }

    private static String valueOf(List<Map.Entry<String, String>> entries, String key) {
        for (Map.Entry<String, String> entry : entries) {
            if (entry.getKey().equals(key)) {
                return entry.getValue();
            }
        }
        return null;
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
