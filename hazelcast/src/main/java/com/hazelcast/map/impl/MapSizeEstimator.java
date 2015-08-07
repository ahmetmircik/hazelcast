/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.serialization.Data;

/**
 * Size estimator for map.
 */
public class MapSizeEstimator implements SizeEstimator {

    private volatile long size;
    private final boolean objectFormatUsed;

    public MapSizeEstimator(InMemoryFormat memoryFormat) {
        this.objectFormatUsed = InMemoryFormat.OBJECT.equals(memoryFormat);
    }

    @Override
    public long getSize() {
        return size;
    }

    @Override
    public void add(long size) {
        this.size += size;
    }

    @Override
    public void reset() {
        size = 0L;
    }

    @Override
    public long calculateSize(Object object) {
        // currently we do not calculate heap cost for InMemoryFormat.OBJECT.
        if (object == null || objectFormatUsed) {
            return 0L;
        }
        final int refCost = 4;

        if (object instanceof Data) {
            long keyCost = ((Data) object).getHeapCost();
            // CHM ref cost of key.
            keyCost += refCost;
            return keyCost;
        }

        if (object instanceof Record) {
            long recordCost = ((Record) object).getCost();
            // CHM ref cost of value.
            recordCost += refCost;
            // CHM ref costs of other.
            recordCost += refCost;
            recordCost += refCost;
            return recordCost;
        }

        return 0L;
    }
}
