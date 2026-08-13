/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.cpmap;

import com.hazelcast.internal.serialization.Data;

import java.util.Objects;

/**
 * Registry-scoped composite key for the snapshot overlay: (map name, key).
 * One overlay freezes the whole registry at a single capture point, so the
 * snapshot scan is consistent ACROSS maps.
 */
public final class CPMapShadowKey {

    private final String mapName;
    private final Data key;

    public CPMapShadowKey(String mapName, Data key) {
        this.mapName = mapName;
        this.key = key;
    }

    public String mapName() {
        return mapName;
    }

    public Data key() {
        return key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CPMapShadowKey that)) {
            return false;
        }
        return mapName.equals(that.mapName) && key.equals(that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mapName, key);
    }
}
