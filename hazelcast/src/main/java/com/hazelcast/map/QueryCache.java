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

package com.hazelcast.map;

import com.hazelcast.map.listener.MapListener;
import com.hazelcast.query.Predicate;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public interface QueryCache<K, V> {

    boolean containsKey(K key);

    boolean containsValue(Object value);

    V get(Object key);

    Map<K, V> getAll(Set<K> keys);

    Set<K> keySet();

    Collection<V> values();

    Set<Map.Entry<K, V>> entrySet();

    Set<K> keySet(Predicate predicate);

    Set<Map.Entry<K, V>> entrySet(Predicate predicate);

    Collection<V> values(Predicate predicate);

    boolean isEmpty();

    int size();

    String addEntryListener(MapListener listener);

    String addEntryListener(MapListener listener, Predicate<K, V> predicate);

    String addEntryListener(MapListener listener, Predicate<K, V> predicate, K key);

    void addIndex(String attribute, boolean ordered);

    String getName();


    V put(K key, V value);

    V remove(Object key);

    boolean remove(Object key, Object value);

    void delete(Object key);

    void synchronize(boolean reload);

    void clear();

    void release();
}

