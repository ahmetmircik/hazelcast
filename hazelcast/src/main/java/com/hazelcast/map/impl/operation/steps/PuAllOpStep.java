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
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.ThreadUtil;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.event.MapEventPublisher;
import com.hazelcast.map.impl.operation.PutAllOperation;
import com.hazelcast.map.impl.recordstore.DefaultRecordStore;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.impl.recordstore.StaticParams;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.core.EntryEventType.ADDED;
import static com.hazelcast.core.EntryEventType.UPDATED;
import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.internal.util.ThreadUtil.isRunningOnPartitionThread;

// FIXME: mapstore load all should be called
// TODO: re-run upon NOOME
public enum PuAllOpStep implements OpStep {

    INITIAL() {
        @Override
        public OpStep nextStep(State state) {
            return READ;
        }
    },

    READ() {
        @Override
        public void runStep(State state) {
            OpStep.log("==> PuAllOpStep reading old value" + " (thread:" + Thread.currentThread().getName() + ")");

            DefaultRecordStore recordStore = ((DefaultRecordStore) state.getRecordStore());
            recordStore.checkIfLoaded();

            MapEntries mapEntries = state.getMapEntries();

            Map batchMap = createHashMap(mapEntries.size());
            List<Map.Entry<Data, Data>> entries = mapEntries.entries();
            for (Map.Entry<Data, Data> entry : entries) {
                batchMap.put(entry.getKey(), entry.getValue());
            }

            state.setResult(batchMap);
        }


        @Override
        public OpStep nextStep(State state) {
            return PuAllOpStep.STORE_ALL;
        }
    },

    STORE_ALL() {
        @Override
        public boolean offloadStep() {
            return true;
        }

        @Override
        public void runStep(State state) {
            assert !isRunningOnPartitionThread();

            OpStep.log("==> PuAllOpStep STORE_ALL " + " (thread:" + Thread.currentThread().getName() + ")");

            DefaultRecordStore recordStore = ((DefaultRecordStore) state.getRecordStore());
            long expirationTime = recordStore.getExpirySystem().calculateExpirationTime(state.getTtl(),
                    state.getMaxIdle(), state.getNow(), state.getNow());
            // TODO what about expiry aware entry-store?
            recordStore.getMapDataStore().storeAll((Map) state.getResult(), expirationTime);

            state.setResult(null);
        }

        @Override
        public OpStep nextStep(State state) {
            return PuAllOpStep.PROCESS;
        }
    },

    PROCESS() {
        @Override
        public void runStep(State state) {
            ThreadUtil.assertRunningOnPartitionThread();

            boolean triggerMapLoader = ((PutAllOperation) state.getOperation()).isTriggerMapLoader();

            MapEntries mapEntries = state.getMapEntries();
            List<Map.Entry<Data, Data>> entries = mapEntries.entries();

            Map<Data, Object> oldValueByKey = new HashMap<>();
            State perKeyState = new State(state);
            boolean loadOldValue = triggerMapLoader
                    && ((PutAllOperation) state.getOperation()).isHasMapListener();
            for (Map.Entry<Data, Data> entry : entries) {

                perKeyState.setKey(entry.getKey())
                        .setNewValue(entry.getValue())
                        .setStaticPutParams(loadOldValue
                                ? StaticParams.PUT_PARAMS : StaticParams.SET_PARAMS);

                PutOpStep.ON_STORE.runStep(perKeyState);

                if (loadOldValue && state.getOldValue() != null) {
                    oldValueByKey.put(state.getKey(), state.getOldValue());
                }
            }

            state.setResult(oldValueByKey);
        }

        @Override
        public OpStep nextStep(State state) {
            return RESPONSE;
        }
    },

    RESPONSE() {
        @Override
        public void runStep(State state) {
            ThreadUtil.assertRunningOnPartitionThread();

            OpStep.log("==> sending response" + " (thread:" + Thread.currentThread().getName() + ")");
            RecordStore recordStore = state.getRecordStore();
            MapContainer mapContainer = recordStore.getMapContainer();
            MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();
            MapEventPublisher mapEventPublisher = mapServiceContext.getMapEventPublisher();
            PutAllOperation operation = (PutAllOperation) state.getOperation();

            List<Map.Entry<Data, Data>> entries = state.getMapEntries().entries();
            for (Map.Entry<Data, Data> entry : entries) {

                Data dataKey = entry.getKey();
                Object newValue = entry.getValue();

                Data dataValue = operation.getValueOrPostProcessedValue(dataKey, (Data) newValue);
                mapServiceContext.interceptAfterPut(mapContainer.getInterceptorRegistry(), dataValue);

                if (operation.isHasMapListener()) {
                    Map<Data, Object> oldValueByKey = (Map<Data, Object>) state.getResult();
                    Object oldValue = oldValueByKey.get(dataKey);
                    EntryEventType eventType = (oldValue == null ? ADDED : UPDATED);
                    mapEventPublisher.publishEvent(state.getCallerAddress(), state.getOperation().getName(),
                            eventType, dataKey, oldValue, dataValue);
                }

                if (operation.isHasWanReplication()) {
                    operation.publishWanUpdate(dataKey, dataValue);
                }

                if (operation.isHasInvalidation()) {
                    operation.getInvalidationKeys().add(dataKey);
                }

                if (operation.isHasBackups()) {
                    operation.getBackupPairs().add(dataKey);
                    operation.getBackupPairs().add(dataValue);
                }

                operation.evict(dataKey);
            }

            operation.sendResponse(true);

            operation.invalidateNearCache(operation.getInvalidationKeys());


            // TODO fix backup op
//            OperationRunner runner = ((OperationServiceImpl) mapServiceContext.getNodeEngine().getOperationService())
//                    .getOperationExecutor().getPartitionOperationRunners()[state.getPartitionId()];
//            ((BasePutOperation) operation).initFields(recordStore.getRecord(state.getKey()),
//                    state.getKey(), mapServiceContext.toData(state.getNewValue()));
//
//            ((OperationRunnerImpl) runner).getBackupHandler().sendBackups(operation);
        }

        @Override
        public OpStep nextStep(State state) {
            return null;
        }
    };

    PuAllOpStep() {
    }
}
