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

package com.hazelcast.map.impl.recordstore;

import java.util.Collection;
import java.util.Set;

public interface InternalRecordStore<K, V> {

    void add(V record);

    V get(K key);

    Object remove(K key);

    boolean containsKey(K key);

    Collection<V> values();

    Set<K> keySet();

    int size();

    boolean isEmpty();

    void clear();

    void destroy();
}
