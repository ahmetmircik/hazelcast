/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.recordstore;

import com.hazelcast.internal.nearcache.impl.invalidation.InvalidationQueue;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.ExceptionUtil;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Future;

// TODO Check only configured number of entries can be run
// TODO load in parallel
public class RefreshAheadLoader<K, V> implements Runnable {

    private final int enqueueThreshold;
    private final String dataStructureName;
    private final ILogger logger;
    private final RecordStoreLoader loader;
    private final InvalidationQueue<Data> refreshRequestQueue = new InvalidationQueue<Data>();

    public RefreshAheadLoader(int enqueueThreshold, String dataStructureName,
                              RecordStoreLoader loader, ILogger logger) {
        this.enqueueThreshold = enqueueThreshold;
        this.dataStructureName = dataStructureName;
        this.logger = logger;
        this.loader = loader;
    }

    public void enqueueRefreshRequest(Data key) {
        if (refreshRequestQueue.size() == enqueueThreshold) {
            return;
        }

        refreshRequestQueue.add(key);
    }


    @Override
    public void run() {
        List<Data> goLoad = new LinkedList<Data>();

        Data key;
        do {
            // TODO max loadable item count
            if ((key = refreshRequestQueue.poll()) != null) {
                goLoad.add(key);
            }
        } while (key != null);

        Future<?> future = loader.loadValues(goLoad, true);

        try {
            future.get();
        } catch (Exception e) {
            logger.severe("Exception while loading values of data structure " + dataStructureName, e);
            throw ExceptionUtil.rethrow(e);
        }
    }
}
