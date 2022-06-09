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

import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;
import com.hazelcast.spi.impl.operationservice.impl.OperationRunnerImpl;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;

public class OpStepBackupHandler {

    public static void handleBackup(State state) {
        MapOperation operation = state.getOperation();
        operation.applyState(state);

        RecordStore recordStore = state.getRecordStore();
        MapContainer mapContainer = recordStore.getMapContainer();
        MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();

        OperationRunner runner = ((OperationServiceImpl) mapServiceContext.getNodeEngine().getOperationService())
                .getOperationExecutor().getPartitionOperationRunners()[state.getPartitionId()];
        ((OperationRunnerImpl) runner).getBackupHandler().sendBackups(operation);
    }
}
