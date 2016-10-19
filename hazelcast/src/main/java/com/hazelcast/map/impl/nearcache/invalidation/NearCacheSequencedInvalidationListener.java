/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.nearcache.invalidation;

import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.logging.ILogger;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class NearCacheSequencedInvalidationListener extends NearCacheInvalidationListener {

    private final String mapName;
    private final Sequencer sequencer;

    public NearCacheSequencedInvalidationListener(String mapName, Sequencer sequencer,
                                                  ILogger logger, NearCache nearCache) {
        super(logger, nearCache);
        this.mapName = mapName;
        this.sequencer = checkNotNull(sequencer, "sequencer cannot be null");
    }

    @Override
    public void handle(SingleNearCacheInvalidation invalidation) {
        super.handle(invalidation);

        handleSequence(invalidation);
    }

    @Override
    public void handle(ClearNearCacheInvalidation invalidation) {
        super.handle(invalidation);

        handleSequence(invalidation);
    }

    private void handleSequence(Invalidation invalidation) {
        long sequence = invalidation.getSequence();

        if (!sequencer.setIfNextSequence(mapName, sequence)) {
            if (logger.isFinestEnabled()) {
                logger.finest(sequencer.currentSequence(mapName) + " but my seq is " + sequence);
            }
        }
    }
}
