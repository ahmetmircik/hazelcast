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

import com.hazelcast.logging.ILogger;

import static com.hazelcast.util.Preconditions.checkNotNull;

class SequencedInvalidationHandler extends InvalidationHandler {

    private final PartitionSequencer sequencer;
    private final InvalidationHandler delegate;
    private final ILogger logger;

    SequencedInvalidationHandler(PartitionSequencer sequencer, InvalidationHandler delegate, ILogger logger) {
        this.sequencer = checkNotNull(sequencer, "sequencer cannot be null");
        this.delegate = checkNotNull(delegate, "delegate cannot be null");
        this.logger = checkNotNull(logger, "logger cannot be null");
    }

    @Override
    public void handle(SingleNearCacheInvalidation invalidation) {
        validateSequence(invalidation);
        delegate.handle(invalidation);
    }

    @Override
    public void handle(ClearNearCacheInvalidation invalidation) {
        validateSequence(invalidation);
        delegate.handle(invalidation);
    }

    private void validateSequence(SequencedInvalidation invalidation) {
        String mapName = invalidation.getName();
        String partitionUuid = invalidation.getSourceUuid();
        long sequence = invalidation.getSequence();
        int partitionId = invalidation.getPartitionId();

        boolean result = sequencer.setOrCheckUuid(partitionId, partitionUuid);
        ///
        if (!sequencer.setIfNextSequence(mapName, partitionId, sequence)) {
            if (logger.isFinestEnabled()) {
                logger.finest(sequencer.currentSequence(mapName, partitionId) + " but my seq is " + sequence);
            }
        }
    }
}
