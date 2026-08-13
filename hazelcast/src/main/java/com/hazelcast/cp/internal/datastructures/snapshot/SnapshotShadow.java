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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Transient write-redirection shadow: the O(1) point-in-time capture
 * mechanism of the snapshot redesign (Option F, "transient shadow freeze").
 * <p>
 * While the shadow is ACTIVE, all writes are redirected to a small
 * {@code overlay} map and the {@code main} map is left untouched, so a
 * snapshot worker can copy {@code main} off-thread with a complete and
 * consistent iteration. Reads consult overlay-then-main (O(1), at most two
 * lookups). When the worker has copied {@code main} and drained the overlay,
 * the single-writer thread deactivates the shadow and writes return to
 * {@code main}.
 * <p>
 * THREAD CONTRACT (mirrors the three-domain threading model):
 * <ul>
 *   <li>capture, put, remove and tryDeactivate are called by the SINGLE
 *       WRITER thread (the Raft/partition thread);</li>
 *   <li>get may be called by any thread;</li>
 *   <li>copyMain and drainOverlay are called by ONE snapshot worker, and
 *       copyMain MUST complete before drainOverlay starts (while the overlay
 *       is active, {@code main} is only written by the drain).</li>
 * </ul>
 * ORDERING INVARIANT (why the flip cannot lose updates): writes stay in the
 * overlay until tryDeactivate observes it empty and atomically switches back.
 * The drain applies to {@code main} FIRST and only then removes the entry
 * from the overlay conditionally ({@code remove(key, expectedValue)}). When
 * the overlay becomes empty, every drained value is already in {@code main};
 * any post-flip write to {@code main} is ordered after the drain's write by
 * the happens-before chain [worker main-write -> worker conditional-remove
 * -> writer isEmpty() observation -> volatile overlay switch -> writer
 * main-write], so the newest value always wins.
 */
public final class SnapshotShadow<K, V> {

    private static final Object TOMBSTONE = new Object();

    private final ConcurrentHashMap<K, Object> main = new ConcurrentHashMap<>();
    private volatile ConcurrentHashMap<K, Object> overlay;

    /**
     * Activates the shadow: O(1). All subsequent writes go to the overlay
     * until {@link #tryDeactivate()} succeeds. A second capture while the
     * shadow is already active is a no-op (coalesced).
     *
     * @return {@code true} if the shadow was activated by this call
     */
    public boolean capture() {
        if (overlay != null) {
            return false;
        }
        overlay = new ConcurrentHashMap<>();
        return true;
    }

    public boolean isShadowActive() {
        return overlay != null;
    }

    /**
     * Write. While the shadow is active, the value goes to the overlay and
     * {@code main} stays frozen; the previous value is not tracked (the
     * snapshot is at the capture point).
     */
    public V put(K key, V value) {
        ConcurrentHashMap<K, Object> activeOverlay = overlay;
        if (activeOverlay != null) {
            activeOverlay.put(key, value);
            return null;
        }
        return toValue(main.put(key, value));
    }

    /**
     * Remove. While the shadow is active, a tombstone is placed in the
     * overlay so reads see the key as absent immediately; the removal is
     * applied to {@code main} when the overlay is drained.
     */
    public V remove(K key) {
        ConcurrentHashMap<K, Object> activeOverlay = overlay;
        if (activeOverlay != null) {
            activeOverlay.put(key, TOMBSTONE);
            return null;
        }
        return toValue(main.remove(key));
    }

    /**
     * Read. O(1): at most two {@code ConcurrentHashMap} lookups while the
     * shadow is active, one otherwise.
     */
    public V get(K key) {
        ConcurrentHashMap<K, Object> activeOverlay = overlay;
        if (activeOverlay != null) {
            Object value = activeOverlay.get(key);
            if (value == TOMBSTONE) {
                return null;
            }
            if (value != null) {
                return toValue(value);
            }
        }
        return toValue(main.get(key));
    }

    /**
     * Copies the frozen {@code main} entries (the point-in-time view at the
     * capture index). MUST be called by the worker before
     * {@link #drainOverlay(Consumer)}; {@code main} is not written until the
     * drain starts, so the iteration is complete and consistent.
     */
    public List<Map.Entry<K, V>> copyMain() {
        List<Map.Entry<K, V>> copy = new ArrayList<>(main.size());
        for (Map.Entry<K, Object> entry : main.entrySet()) {
            Object value = entry.getValue();
            if (value != TOMBSTONE) {
                copy.add(new AbstractMap.SimpleImmutableEntry<>(entry.getKey(), toValue(value)));
            }
        }
        return copy;
    }

    /**
     * Consumer callback for each drained overlay entry.
     */
    public interface Consumer<K, V> {
        void accept(K key, V value, boolean tombstone);
    }

    /**
     * Drains the overlay into {@code main}, applying the
     * main-first / conditional-remove ordering that preserves the ordering
     * invariant. Loops until it observes the overlay empty; a concurrent
     * write may re-add an entry, which is drained on a subsequent pass.
     * Called by ONE snapshot worker; the worker then notifies the single
     * writer, which calls {@link #tryDeactivate()}.
     *
     * @return the number of entries drained by this call
     */
    public int drainOverlay(Consumer<K, V> consumer) {
        ConcurrentHashMap<K, Object> activeOverlay = overlay;
        if (activeOverlay == null) {
            return 0;
        }
        int drained = 0;
        while (true) {
            Map.Entry<K, Object> next = firstEntry(activeOverlay);
            if (next == null) {
                break;
            }
            K key = next.getKey();
            Object value = next.getValue();
            if (value == TOMBSTONE) {
                // apply to main first, then remove from the overlay
                // conditionally: if a newer write landed meanwhile, leave it
                // in the overlay for the next pass.
                main.remove(key);
                if (activeOverlay.remove(key, TOMBSTONE)) {
                    consumer.accept(key, null, true);
                    drained++;
                }
            } else {
                main.put(key, value);
                if (activeOverlay.remove(key, value)) {
                    consumer.accept(key, toValue(value), false);
                    drained++;
                }
            }
        }
        return drained;
    }

    /**
     * Deactivates the shadow when the overlay is observed empty: O(1) on the
     * single-writer thread. After a successful deactivation, writes go to
     * {@code main} and reads stop consulting the overlay.
     *
     * @return {@code true} if the shadow was deactivated by this call
     */
    public boolean tryDeactivate() {
        ConcurrentHashMap<K, Object> activeOverlay = overlay;
        if (activeOverlay == null) {
            return true;
        }
        if (activeOverlay.isEmpty()) {
            overlay = null;
            return true;
        }
        return false;
    }

    /**
     * Size of the frozen {@code main} map (used for chunk estimation at
     * capture time; only meaningful before the drain starts).
     */
    public int mainSize() {
        return main.size();
    }

    /**
     * Number of pending overlay entries (memory-growth visibility; bounded by
     * the snapshot-lag backpressure in the real pipeline).
     */
    public int overlaySize() {
        ConcurrentHashMap<K, Object> activeOverlay = overlay;
        return activeOverlay == null ? 0 : activeOverlay.size();
    }

    private static <K> Map.Entry<K, Object> firstEntry(ConcurrentHashMap<K, Object> map) {
        for (Map.Entry<K, Object> entry : map.entrySet()) {
            return entry;
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static <V> V toValue(Object value) {
        return (V) value;
    }
}
