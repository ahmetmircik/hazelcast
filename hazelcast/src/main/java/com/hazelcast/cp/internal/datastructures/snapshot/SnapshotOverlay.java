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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Transient write-redirection overlay for an EXTERNAL "main" state (the
 * snapshot-redesign Option F mechanism, the sibling of {@link SnapshotShadow}
 * for state that lives in external maps, e.g. a {@code CPMapRegistry}).
 * <p>
 * While the overlay is ACTIVE, all writes are redirected here and the
 * external main state is left untouched, so a snapshot worker can scan the
 * main state off-thread with a complete and consistent iteration. Reads
 * consult overlay-then-main. When the worker finished scanning, the
 * single-writer thread applies the overlay back to the main state and
 * deactivates it.
 * <p>
 * THREAD CONTRACT:
 * <ul>
 *   <li>{@link #capture()}, {@link #put(K, V)}, {@link #remove(K)} and
 *       {@link #finish(BiConsumer, BiConsumer)} are called by the SINGLE
 *       WRITER thread (the Raft thread);</li>
 *   <li>{@link #get(K, Function)} may be called by any thread;</li>
 *   <li>the main state is only mutated by {@link #finish(...)} (single
 *       writer), after the worker's scan completed.</li>
 * </ul>
 * Because capture and finish both run on the single writer thread, the
 * overlay can never lose an update: writes stay here until {@code finish}
 * applies them back to the main state in the same critical section.
 */
public final class SnapshotOverlay<K, V> {

    private static final Object TOMBSTONE = new Object();

    private final ConcurrentHashMap<K, Object> overlay = new ConcurrentHashMap<>();
    private volatile boolean active;

    /**
     * Activates the overlay: O(1). All subsequent writes are redirected here
     * until {@link #finish(BiConsumer, BiConsumer)}.
     *
     * @return {@code true} if activated by this call (a second capture is a
     *         no-op)
     */
    public boolean capture() {
        if (active) {
            return false;
        }
        active = true;
        return true;
    }

    public boolean isActive() {
        return active;
    }

    /**
     * Write: redirected to the overlay (the main state stays frozen). Must be
     * called on the single-writer thread while active.
     */
    public void put(K key, V value) {
        overlay.put(key, value);
    }

    /**
     * Remove: a tombstone is placed in the overlay so reads see the key as
     * absent immediately; the removal is applied to the main state by
     * {@link #finish(BiConsumer, BiConsumer)}.
     */
    public void remove(K key) {
        overlay.put(key, TOMBSTONE);
    }

    /**
     * Read: overlay-then-main. O(1), at most two lookups while active.
     */
    public V get(K key, Function<K, V> mainGet) {
        Object value = overlay.get(key);
        if (value == TOMBSTONE) {
            return null;
        }
        if (value != null) {
            return (V) value;
        }
        return mainGet.apply(key);
    }

    /**
     * Applies the overlay back to the main state and deactivates it. Must be
     * called on the single-writer thread (after the worker's scan finished):
     * no concurrent writes can interleave, so no update can be lost and no
     * ordering issue can arise.
     *
     * @param putMain    applies a key/value pair to the main state
     * @param removeMain applies a key removal to the main state
     */
    public void finish(BiConsumer<K, V> putMain, BiConsumer<K, V> removeMain) {
        if (!active) {
            return;
        }
        for (Map.Entry<K, Object> entry : overlay.entrySet()) {
            K key = entry.getKey();
            Object value = entry.getValue();
            if (value == TOMBSTONE) {
                removeMain.accept(key, null);
            } else {
                putMain.accept(key, (V) value);
            }
        }
        overlay.clear();
        active = false;
    }

    /**
     * Number of pending redirected writes (memory-growth visibility; bounded
     * by the snapshot-lag backpressure in the real pipeline).
     */
    public int size() {
        return active ? overlay.size() : 0;
    }
}
