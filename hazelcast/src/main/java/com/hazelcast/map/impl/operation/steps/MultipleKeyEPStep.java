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

package com.hazelcast.map.impl.operation.steps;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.operation.EntryOperator;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.DefaultRecordStore;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.hazelcast.map.impl.operation.EntryOperator.operator;

// TODO: what happens if partition is migrating and we offload map load.
public enum MultipleKeyEPStep implements Step<State> {

    INITIAL() {
        @Override
        public Step nextStep(State state) {
            return FIND_KEYS_TO_LOAD;
        }
    },

    FIND_KEYS_TO_LOAD() {
        @Override
        public Runnable createStep(State state) {
            return new PartitionSpecificRunnable() {
                @Override
                public void run() {
                    Step.log("==> EP start" + " (thread:" + Thread.currentThread().getName() + ")");

                    RecordStore recordStore = state.getRecordStore();
                    IPartitionService partitionService = recordStore.getMapContainer()
                            .getMapServiceContext().getNodeEngine().getPartitionService();

                    List<Data> keysToLoad = new ArrayList<>();
                    Collection<Data> keys = state.getKeys();
                    for (Data key : keys) {
                        if (partitionService.getPartitionId(key) == state.getPartitionId()
                                && recordStore.getRecord(key) == null) {
                            keysToLoad.add(key);
                        }
                    }

                    if (!keysToLoad.isEmpty()) {
                        state.setKeysToLoad(Collections.unmodifiableList(keysToLoad));
                    }
                }

                @Override
                public int getPartitionId() {
                    return state.getPartitionId();
                }
            };
        }

        @Override
        public Step nextStep(State state) {
            if (!state.getKeysToLoad().isEmpty()) {
                return MultipleKeyEPStep.LOAD_ALL;
            }
            // FIXME
            return MultipleKeyEPStep.PROCESS;
        }
    },

    LOAD_ALL() {
        @Override
        public Runnable createStep(State state) {
            return () -> {
                Collection<Data> keysToLoad = state.getKeysToLoad();
                state.setLoadedKeyValuePairs(state.getRecordStore().getMapDataStore().loadAll(keysToLoad));
            };
        }

        @Override
        public Step nextStep(State state) {
            return state.getLoadedKeyValuePairs().isEmpty()
                    ? MultipleKeyEPStep.PROCESS : MultipleKeyEPStep.ON_LOAD_ALL;
        }
    },

    ON_LOAD_ALL() {
        @Override
        public Runnable createStep(State state) {
            return new PartitionSpecificRunnable() {
                @Override
                public void run() {
                    Step.log("==> on old all"
                            + " (thread:" + Thread.currentThread().getName() + ")");

                    RecordStore recordStore = state.getRecordStore();
                    MapContainer mapContainer = recordStore.getMapContainer();
                    MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();

                    // create record for loaded records.
                    Map<Object, Object> loadedKeyValuePairs = state.getLoadedKeyValuePairs();
                    for (Map.Entry<Object, Object> entry : loadedKeyValuePairs.entrySet()) {
                        Object key = entry.getKey();
                        if (key == null) {
                            continue;
                        }
                        Object value = entry.getValue();

                        ((DefaultRecordStore) recordStore)
                                .onLoadRecord(mapServiceContext.toData(key), value, false, state.getCallerAddress());
                    }

                }

                @Override
                public int getPartitionId() {
                    return state.getPartitionId();
                }
            };
        }

        @Override
        public Step nextStep(State state) {
            return MultipleKeyEPStep.PROCESS;
        }
    },

    PROCESS() {
        @Override
        public Runnable createStep(State state) {
            return new PartitionSpecificRunnable() {
                @Override
                public void run() {
                    Step.log("==> processing entry" + " (thread:" + Thread.currentThread().getName() + ")");
                    RecordStore recordStore = state.getRecordStore();

                    List<State> toStore = new ArrayList<>();
                    List<State> toRemove = new ArrayList<>();

                    state.setToStore(toStore);
                    state.setToRemove(toRemove);

                    MapEntries responses = new MapEntries(state.getKeys().size());
                    state.setResult(responses);

                    Collection<Data> keys = state.getKeys();
                    for (Data key : keys) {
                        Record record = recordStore.getRecord(key);

                        // create specific state for this key
                        State singleKeyState = new State(state);
                        singleKeyState
                                .setKey(key)
                                .setOldValue(record == null ? null : record.getValue())
                                .setEntryOperator(operator(singleKeyState.getOperation(),
                                        singleKeyState.getEntryProcessor(), singleKeyState.getPredicate()));

                        EPStep.PROCESS.runStepDirect(singleKeyState);

                        EntryEventType eventType = singleKeyState.getOperator().getEventType();
                        if (eventType == null) {
                            Data result = singleKeyState.getOperator().getResult();
                            if (result != null) {
                                responses.add(singleKeyState.getKey(), result);
                            }
                        } else {
                            switch (eventType) {
                                case ADDED:
                                case UPDATED:
                                    toStore.add(singleKeyState);
                                    break;
                                case REMOVED:
                                    toRemove.add(singleKeyState);
                                    break;
                                default:
                                    throw new IllegalArgumentException("Unexpected event found:" + eventType);
                            }
                        }
                    }
                }

                @Override
                public int getPartitionId() {
                    return state.getPartitionId();
                }
            };
        }

        @Override
        public Step nextStep(State state) {
            return !state.getToStore().isEmpty() || !state.getToRemove().isEmpty()
                    ? STORE_OR_DELETE : ON_STORE_OR_DELETE;
        }
    },

    STORE_OR_DELETE() {
        @Override
        public Runnable createStep(State state) {
            return () -> {
                List<State> toStore = state.getToStore();
                for (State s : toStore) {
                    PutStep.STORE.runStepDirect(s);
                }

                List<State> toRemove = state.getToRemove();
                for (State s : toRemove) {
                    DeleteStep.DELETE.runStepDirect(s);
                }
            };
        }

        @Override
        public Step nextStep(State state) {
            return ON_STORE_OR_DELETE;
        }
    },

    ON_STORE_OR_DELETE() {
        @Override
        public Runnable createStep(State state) {
            return new PartitionSpecificRunnable() {
                @Override
                public void run() {
                    List<State> toStore = state.getToStore();
                    for (State s : toStore) {
                        PutStep.ON_STORE.runStepDirect(s);

                        EntryOperator operator = s.getOperator();
                        operator.onAddedOrUpdated0(s.getNewValue());
                        operator.doPostOperateOps0();

                        Data result = s.getOperator().getResult();
                        if (result != null) {
                            ((MapEntries) state.getResult()).add(s.getKey(), result);
                        }
                    }

                    List<State> toRemove = state.getToRemove();
                    for (State s : toRemove) {
                        DeleteStep.ON_DELETE.runStepDirect(s);

                        EntryOperator operator = s.getOperator();
                        operator.onRemove0();
                        operator.doPostOperateOps0();

                        Data result = s.getOperator().getResult();
                        if (result != null) {
                            ((MapEntries) state.getResult()).add(s.getKey(), result);
                        }
                    }

                    Operation operation = state.getOperation();
                    operation.sendResponse(state.getResult());
                }

                @Override
                public int getPartitionId() {
                    return state.getPartitionId();
                }
            };
        }

        @Override
        public Step nextStep(State state) {
            return null;
        }
    };

    MultipleKeyEPStep() {
    }
}
