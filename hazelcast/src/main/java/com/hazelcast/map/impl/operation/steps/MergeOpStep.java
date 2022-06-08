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

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.ThreadUtil;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.event.MapEventPublisher;
import com.hazelcast.map.impl.operation.MergeOperation;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.DefaultRecordStore;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.impl.recordstore.StaticParams;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.core.EntryEventType.MERGED;
import static com.hazelcast.internal.util.ThreadUtil.isRunningOnPartitionThread;
import static com.hazelcast.spi.impl.merge.MergingValueFactory.createMergingEntry;

// TODO: re-run upon NOOME
public enum MergeOpStep implements OpStep {

    INITIAL() {
        @Override
        public OpStep nextStep(State state) {
            return READ;
        }
    },

    READ() {
        @Override
        public void runStep(State state) {
            OpStep.log("==> MergeOpStep reading old value" + " (thread:" + Thread.currentThread().getName() + ")");

            DefaultRecordStore recordStore = ((DefaultRecordStore) state.getRecordStore());
            MapContainer mapContainer = recordStore.getMapContainer();
            MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();
            SerializationService serializationService = mapServiceContext.getNodeEngine().getSerializationService();
            List<SplitBrainMergeTypes.MapMergeTypes<Object, Object>> mergingEntries = state.getMergingEntries();
            SplitBrainMergePolicy<Object, SplitBrainMergeTypes.MapMergeTypes<Object, Object>, Object> mergePolicy = state.getMergePolicy();

            MergeOperation.checkMergePolicy(mapContainer, mergePolicy);

            // TODO index marking
            // key + oldValue + newValue
            List outcomes = new ArrayList<>(3 * mergingEntries.size());
            state.setResult(outcomes);

            for (SplitBrainMergeTypes.MapMergeTypes<Object, Object> mergingEntry : mergingEntries) {
                mergingEntry = (SplitBrainMergeTypes.MapMergeTypes<Object, Object>) serializationService
                        .getManagedContext().initialize(mergingEntry);
                mergePolicy = (SplitBrainMergePolicy<Object, SplitBrainMergeTypes.MapMergeTypes<Object, Object>, Object>)
                        serializationService.getManagedContext().initialize(mergePolicy);
                Data key = (Data) mergingEntry.getRawKey();
                Record record = recordStore.getRecordOrNull(key, state.getNow(), false);

                SplitBrainMergeTypes.MapMergeTypes<Object, Object> existingEntry = record != null
                        ? createMergingEntry(serializationService, key, record,
                        recordStore.getExpirySystem().getExpiryMetadata(key)) : null;

                Object oldValue = record == null ? null : record.getValue();
                Object newValue = mergePolicy.merge(mergingEntry, existingEntry);

                if (oldValue == null && newValue == null) {
                    continue;
                }

                outcomes.add(key);
                outcomes.add(oldValue);
                outcomes.add(newValue);
            }
        }

        @Override
        public OpStep nextStep(State state) {
            boolean persistenceEnabled = ((DefaultRecordStore) state.getRecordStore())
                    .persistenceEnabledFor(state.getCallerProvenance());

            List outcomes = (List) state.getResult();
            return !outcomes.isEmpty() && persistenceEnabled
                    ? MergeOpStep.STORE_OR_DELETE : MergeOpStep.PROCESS;
        }
    },

    STORE_OR_DELETE() {
        @Override
        public boolean offloadStep() {
            return true;
        }

        @Override
        public void runStep(State state) {
            assert !isRunningOnPartitionThread();

            OpStep.log("==> MergeOpStep STORE_OR_DELETE " + " (thread:" + Thread.currentThread().getName() + ")");

            List outcomes = (List) state.getResult();
            State perKeyState = new State(state);
            for (int i = 0; i < outcomes.size(); i++) {
                Object key = outcomes.get(i++);

                assert key instanceof Data
                        : "Expect key instanceOf Data but found " + key;

                Object oldValue = outcomes.get(i++);
                Object newValue = outcomes.get(i++);

                perKeyState.setKey((Data) key)
                        .setOldValue(oldValue)
                        .setNewValue(newValue);

                if (oldValue == null && newValue != null
                        || oldValue != null && newValue != null) {
                    // put or update
                    PutOpStep.STORE.runStep(perKeyState);
                    //set new value returned from map-store
                    outcomes.set(i - 1, perKeyState.getNewValue());
                } else if (oldValue != null && newValue == null) {
                    // remove
                    DeleteOpStep.DELETE.runStep(perKeyState);
                }
            }

            state.setResult(outcomes);
        }

        @Override
        public OpStep nextStep(State state) {
            return MergeOpStep.PROCESS;
        }
    },

    PROCESS() {
        @Override
        public void runStep(State state) {
            ThreadUtil.assertRunningOnPartitionThread();

            DefaultRecordStore recordStore = (DefaultRecordStore) state.getRecordStore();
            MapContainer mapContainer = recordStore.getMapContainer();
            MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();
            SerializationService serializationService = mapServiceContext
                    .getNodeEngine().getSerializationService();

            List outcomes = (List) state.getResult();
            State perKeyState = new State(state);
            for (int i = 0; i < outcomes.size(); i++) {
                Object key = outcomes.get(i++);

                assert key instanceof Data
                        : "Expect key instanceOf Data but found " + key;

                Object oldValue = outcomes.get(i++);
                Object newValue = outcomes.get(i++);

                perKeyState.setKey((Data) key)
                        .setOldValue(oldValue)
                        .setNewValue(newValue)
                        .setStaticPutParams(StaticParams.PUT_PARAMS);

                if (oldValue == null && newValue != null
                        || oldValue != null && newValue != null) {

                    // if same values, merge expiry and continue with next entry
                    // TODO add expiry merge
                    if (recordStore.getValueComparator().isEqual(newValue, oldValue, serializationService)) {
//                        recordStore.mergeRecordExpiration(key, record, mergingEntry, state.getNow());
                        continue;
                    }

                    // put or update
                    PutOpStep.ON_STORE.runStep(perKeyState);
                } else if (oldValue != null && newValue == null) {
                    // remove
                    DeleteOpStep.ON_DELETE.runStep(perKeyState);
                }
            }
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
            MergeOperation operation = ((MergeOperation) state.getOperation());

            List<Data> invalidationKeys = null;
            boolean hasMergedValues = false;

            List backupPairs = null;

            boolean hasMapListener = mapEventPublisher.hasEventListener(state.getOperation().getName());
            boolean hasWanReplication = mapContainer.isWanReplicationEnabled()
                    && !state.isDisableWanReplicationEvent();
            boolean hasBackups = mapContainer.getTotalBackupCount() > 0;
            boolean hasInvalidation = mapContainer.hasInvalidationListener();

            List outcomes = (List) state.getResult();

            if (hasBackups) {
                backupPairs = new ArrayList(2 * (outcomes.size() / 3));
            }

            if (hasInvalidation) {
                invalidationKeys = new ArrayList<>((outcomes.size() / 3));
            }

            for (int i = 0; i < outcomes.size(); i += 3) {
                hasMergedValues = true;

                Data dataKey = ((Data) outcomes.get(i));

                Object oldValue = outcomes.get(i + 1);
                // TODO can't we use this newValue directly.
                Object newValue = outcomes.get(i + 2);

                Data dataValue = operation.getValueOrPostProcessedValue(dataKey, operation.getValue(dataKey));
                mapServiceContext.interceptAfterPut(mapContainer.getInterceptorRegistry(), dataValue);

                if (hasMapListener) {
                    mapEventPublisher.publishEvent(state.getCallerAddress(), state.getOperation().getName(),
                            MERGED, dataKey, oldValue, dataValue);
                }

                if (hasWanReplication) {
                    operation.publishWanUpdate(dataKey, dataValue);
                }

                if (hasInvalidation) {
                    invalidationKeys.add(dataKey);
                }

                if (hasBackups) {
                    backupPairs.add(dataKey);
                    backupPairs.add(dataValue);
                }

                operation.evict(dataKey);
            }

            operation.sendResponse(hasMergedValues);

            operation.invalidateNearCache(invalidationKeys);


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

    MergeOpStep() {
    }
}
