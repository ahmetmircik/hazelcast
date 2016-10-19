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

import java.util.List;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class NearCacheInvalidationListener implements InvalidationListener, InvalidationHandler {

    protected final ILogger logger;
    protected final NearCache nearCache;

    public NearCacheInvalidationListener(ILogger logger, NearCache nearCache) {
        this.logger = checkNotNull(logger, "logger cannot be null");
        this.nearCache = checkNotNull(nearCache, "nearCache cannot be null");
    }

    @Override
    public void onInvalidate(Invalidation invalidation) {
        assert invalidation != null;

        invalidation.consumedBy(this);
    }

    @Override
    public void handle(BatchNearCacheInvalidation batch) {
        List<Invalidation> invalidations = batch.getInvalidations();

        for (Invalidation invalidation : invalidations) {
            invalidation.consumedBy(this);
        }
    }

    @Override
    public void handle(SingleNearCacheInvalidation invalidation) {
        nearCache.remove(invalidation.getKey());
    }

    @Override
    public void handle(ClearNearCacheInvalidation invalidation) {
        nearCache.clear();
    }
}
