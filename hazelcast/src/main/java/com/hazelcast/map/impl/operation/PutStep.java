/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.operation;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.internal.util.comparators.ValueComparator;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.recordstore.DefaultRecordStore;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.impl.recordstore.StaticParams;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;

public enum PutStep {

    LOAD() {
        @Override
        Runnable createExecutionWith(State state) {
            return () -> {
                System.out.println("==> loading old value");
                state.setOldValue(state.getRecordStore().getMapDataStore().load(state.getKey()));
            };
        }
    },

    AFTER_LOAD() {
        @Override
        Runnable createExecutionWith(State state) {
            return new PartitionSpecificRunnable() {
                @Override
                public void run() {
                    System.out.println("==> meta processing");
                    StaticParams staticParams = state.getStaticParams();
                    Object newValue = state.getNewValue();

                    // For method replace, if current value is not expected one, return.
                    RecordStore recordStore = state.getRecordStore();
                    MapContainer mapContainer = recordStore.getMapContainer();
                    MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();

                    ValueComparator valueComparator = ((DefaultRecordStore) recordStore).getValueComparator();
                    if (staticParams.isPutIfEqual()
                            && !valueComparator.isEqual(state.getExpect(), state.getOldValue(),
                            mapServiceContext.getNodeEngine().getSerializationService())) {
                        return;
                    }

                    // Intercept put on owner partition.
                    if (!staticParams.isBackup()) {
                        newValue = mapServiceContext.interceptPut(mapContainer.getInterceptorRegistry(),
                                state.getOldValue(), newValue);
                        state.setNewValue(newValue);
                    }
                }

                @Override
                public int getPartitionId() {
                    return state.getPartitionId();
                }
            };
        }
    },

    STORE() {
        @Override
        Runnable createExecutionWith(State state) {
            return () -> {
                System.out.println("==> store into map-store");
                ((DefaultRecordStore) state.getRecordStore()).putIntoMapStore2(state.getKey(),
                        state.getNewValue(), state.getTtl(), state.getMaxIdle(), state.getNow(), state.getTxnId());
            };
        }
    },

    WRITE() {
        @Override
        Runnable createExecutionWith(State state) {
            return new PartitionSpecificRunnable() {
                @Override
                public void run() {
                    System.out.println("==> write into memory");
                    // TODO: fix event type
                    ((DefaultRecordStore) state.getRecordStore()).writeMemory(state.getKey(), state.getOldValue(),
                            state.getNewValue(), state.getTtl(), state.getMaxIdle(), state.getExpiryTime(),
                            state.getNow(), EntryEventType.ADDED, state.staticParams.isBackup(), null);
                }

                @Override
                public int getPartitionId() {
                    return state.getPartitionId();
                }
            };
        }
    },

    RESPONSE() {
        @Override
        Runnable createExecutionWith(State state) {
            return () -> {
                System.out.println("==> sending response");
                RecordStore recordStore = state.getRecordStore();
                MapContainer mapContainer = recordStore.getMapContainer();
                MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();

                state.getOperation().sendResponse(mapServiceContext.toData(state.oldValue));
            };
        }
    };

    public static final PutStep[] VALUES = values();

    PutStep() {
    }

    abstract Runnable createExecutionWith(State state);
}
