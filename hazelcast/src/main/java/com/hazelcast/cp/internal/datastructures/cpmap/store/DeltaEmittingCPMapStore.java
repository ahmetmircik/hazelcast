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

package com.hazelcast.cp.internal.datastructures.cpmap.store;

import com.hazelcast.cp.internal.datastructures.cpmap.CPMapShadowKey;
import com.hazelcast.cp.internal.datastructures.snapshot.DeltaContext;
import com.hazelcast.cp.internal.datastructures.snapshot.SnapshotOverlay;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.serialization.Data;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

/**
 * {@link CPMapStore} decorator that emits key-level snapshot deltas from the
 * ACTUAL outcome of each mutation, at the mutation point (Option B/F of the
 * snapshot redesign). It keeps the underlying store's semantics untouched and
 * adds O(1) emission per op:
 * <ul>
 *   <li>{@link #put} — unconditional PUT delta;</li>
 *   <li>{@link #putIfAbsent} — PUT delta only when the key was absent
 *       ({@code prev == null});</li>
 *   <li>{@link #compareAndSet} — PUT delta only when the comparison
 *       succeeded;</li>
 *   <li>{@link #remove} — DELETE delta only when the key existed;</li>
 *   <li>{@link #purge} — one DELETE delta per removed key (contract: the
 *       delegate's purge result must enumerate the removed keys).</li>
 * </ul>
 * Emission happens only when a {@link DeltaContext} is active on the calling
 * thread. The coordinator opens a context during {@code applyLogEntry} and
 * NEVER during snapshot install/restore, so restore cannot double-emit.
 * <p>
 * Double-emission is impossible by construction: the decorator delegates to
 * the REAL store, so internal calls made by the delegate (e.g.
 * {@code HeapCPMapStore.putIfAbsent} calling {@code put}) hit the delegate,
 * never this decorator; each top-level store call emits at most once.
 */
public class DeltaEmittingCPMapStore implements CPMapStore {

    private final CPMapStore delegate;
    private final String serviceName;
    private final String objectName;

    /**
     * The registry snapshot overlay, or null when async snapshot capture is
     * not wired. While the overlay is active, mutations are redirected to it
     * (the real store stays frozen for the off-thread snapshot scan) and
     * reads consult overlay-then-store.
     */
    private final SnapshotOverlay<CPMapShadowKey, Data> overlay;

    public DeltaEmittingCPMapStore(String serviceName, String objectName, CPMapStore delegate) {
        this(serviceName, objectName, delegate, null);
    }

    public DeltaEmittingCPMapStore(String serviceName, String objectName, CPMapStore delegate,
                                   SnapshotOverlay<CPMapShadowKey, Data> overlay) {
        this.serviceName = serviceName;
        this.objectName = objectName;
        this.delegate = delegate;
        this.overlay = overlay;
    }

    private boolean overlayActive() {
        return overlay != null && overlay.isActive();
    }

    private CPMapShadowKey shadowKey(Data key) {
        return new CPMapShadowKey(objectName, key);
    }

    @Override
    public Data put(@Nonnull Data key, @Nonnull Data value, long timestamp) {
        Data previous;
        if (overlayActive()) {
            CPMapShadowKey shadowKey = shadowKey(key);
            previous = overlay.get(shadowKey, ignored -> delegate.get(key));
            overlay.put(shadowKey, value);
        } else {
            previous = delegate.put(key, value, timestamp);
        }
        emitPut(key, value, timestamp);
        return previous;
    }

    @Override
    public Data putIfAbsent(@Nonnull Data key, @Nonnull Data value, long timestamp) {
        Data previous;
        if (overlayActive()) {
            CPMapShadowKey shadowKey = shadowKey(key);
            previous = overlay.get(shadowKey, ignored -> delegate.get(key));
            if (previous == null) {
                overlay.put(shadowKey, value);
            }
        } else {
            previous = delegate.putIfAbsent(key, value, timestamp);
        }
        if (previous == null) {
            emitPut(key, value, timestamp);
        }
        return previous;
    }

    @Override
    public boolean compareAndSet(@Nonnull Data key, @Nonnull Data expectedValue,
                                 @Nonnull Data newValue, long timestamp) {
        boolean updated;
        if (overlayActive()) {
            CPMapShadowKey shadowKey = shadowKey(key);
            Data observed = overlay.get(shadowKey, ignored -> delegate.get(key));
            updated = Objects.equals(observed, expectedValue);
            if (updated) {
                overlay.put(shadowKey, newValue);
            }
        } else {
            updated = delegate.compareAndSet(key, expectedValue, newValue, timestamp);
        }
        if (updated) {
            emitPut(key, newValue, timestamp);
        }
        return updated;
    }

    @Override
    public Data remove(@Nonnull Data key) {
        Data removed;
        if (overlayActive()) {
            CPMapShadowKey shadowKey = shadowKey(key);
            removed = overlay.get(shadowKey, ignored -> delegate.get(key));
            overlay.remove(shadowKey);
        } else {
            removed = delegate.remove(key);
        }
        if (removed != null) {
            emitDelete(key);
        }
        return removed;
    }

    @Override
    public Object purge(long ageMillis, long timestamp) {
        Object result = delegate.purge(ageMillis, timestamp);
        if (result instanceof Collection<?> removedKeys) {
            for (Object removedKey : removedKeys) {
                if (removedKey instanceof Data key) {
                    emitDelete(key);
                }
            }
        }
        return result;
    }

    @Override
    public Data get(@Nonnull Data key) {
        if (overlayActive()) {
            return overlay.get(shadowKey(key), ignored -> delegate.get(key));
        }
        return delegate.get(key);
    }

    @Override
    public Iterator<Map.Entry<Data, Data>> iterator() {
        return delegate.iterator();
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public boolean isPurgeEnabled() {
        return delegate.isPurgeEnabled();
    }

    @Override
    public Map<Data, Long> getEntryTimestamps() {
        return delegate.getEntryTimestamps();
    }

    @Override
    public void collectMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        delegate.collectMetrics(descriptor, context);
    }

    private void emitPut(Data key, Data value, long timestamp) {
        DeltaContext ctx = DeltaContext.current();
        if (ctx != null) {
            ctx.sink().put(serviceName, objectName, key, value, timestamp);
        }
    }

    private void emitDelete(Data key) {
        DeltaContext ctx = DeltaContext.current();
        if (ctx != null) {
            ctx.sink().delete(serviceName, objectName, key);
        }
    }
}
