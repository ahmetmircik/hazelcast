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

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Verifies the external-main write-redirection overlay used by the async
 * chunked snapshot generation (Option F): O(1) capture freezes the main
 * state for an off-thread scan, reads see overlay-then-main, and finish
 * applies the redirected writes back without losing any update.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SnapshotOverlayTest {

    private final Map<String, String> main = new HashMap<>();
    private final SnapshotOverlay<String, String> overlay = new SnapshotOverlay<>();

    @Test
    public void capture_redirectsWrites_mainStaysFrozen() {
        main.put("k", "v1");

        assertTrue(overlay.capture());
        assertFalse(overlay.capture()); // already active
        assertTrue(overlay.isActive());

        overlay.put("k", "v2");
        overlay.put("k2", "v2");

        // main is frozen: a worker scan sees exactly the capture point
        assertEquals(1, main.size());
        assertEquals("v1", main.get("k"));

        // reads see overlay-then-main
        assertEquals("v2", overlay.get("k", main::get));
        assertEquals("v2", overlay.get("k2", main::get));
    }

    @Test
    public void finish_appliesOverlayBack_andDeactivates() {
        main.put("k", "v1");
        overlay.capture();
        overlay.put("k", "v2");
        overlay.put("k2", "v2");

        overlay.finish(main::put, (key, ignored) -> main.remove(key));

        assertFalse(overlay.isActive());
        assertEquals(0, overlay.size());
        assertEquals("v2", main.get("k"));
        assertEquals("v2", main.get("k2"));
    }

    @Test
    public void remove_duringCapture_isVisibleImmediately_andAppliedOnFinish() {
        main.put("k", "v1");
        overlay.capture();

        overlay.remove("k");
        assertNull(overlay.get("k", main::get));
        assertEquals("v1", main.get("k")); // frozen

        overlay.finish(main::put, (key, ignored) -> main.remove(key));
        assertNull(main.get("k"));
    }

    @Test
    public void finish_noLostUpdate() {
        // design test F8 equivalent for the overlay: writes during the
        // capture are applied back on finish, so the newest write always wins
        main.put("k", "v1");
        overlay.capture();
        overlay.put("k", "v2"); // redirected during the capture
        overlay.finish(main::put, (key, ignored) -> main.remove(key));
        main.put("k", "v3"); // post-finish write goes straight to main
        assertEquals("v3", main.get("k"));
    }

    @Test
    public void finish_whenNeverCaptured_isNoOp() {
        main.put("k", "v1");
        overlay.finish(main::put, (key, ignored) -> main.remove(key));
        assertFalse(overlay.isActive());
        assertEquals("v1", main.get("k"));
    }

    @Test
    public void inactiveOverlay_behavesAsPassThrough() {
        main.put("k", "v1");
        assertEquals("v1", overlay.get("k", main::get));
        assertNull(overlay.get("missing", main::get));
    }
}
