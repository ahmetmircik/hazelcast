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

package com.hazelcast.map.impl.mapstore;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapStoreAdapter;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MapStoreOffloadingTest extends HazelcastTestSupport {

    @Test
    public void name2() {
        HazelcastInstance node = createHazelcastInstance();
        IMap<Object, Object> test1 = node.getMap("test1");
        for (int i = 0; i < 1; i++) {
            test1.set(i, i);
        }

        sleepSeconds(10);
    }

    @Test
    public void smokePut() {
        Config config = getConfig();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "1");
        config.setProperty(ClusterProperty.PARTITION_OPERATION_THREAD_COUNT.getName(), "1");

        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(new MapStoreAdapter<Integer, Integer>() {

            ConcurrentHashMap store = new ConcurrentHashMap<>();

            @Override
            public Integer load(Integer key) {
                sleepSeconds(2);
                return (Integer) store.get(key);
            }

            @Override
            public void store(Integer key, Integer value) {
                sleepSeconds(2);
                store.put(key, value);
            }

            @Override
            public Map<Integer, Integer> loadAll(Collection<Integer> keys) {
                return null;
            }

            @Override
            public Iterable<Integer> loadAllKeys() {
                return null;
            }
        });
        String slowMapName = "slowMap";
        config.getMapConfig(slowMapName).setMapStoreConfig(mapStoreConfig);

        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Object, Object> slowMap = node.getMap(slowMapName);

        Object old = slowMap.put(1, 1);
        System.out.println("old = " + old);

        Object old2 = slowMap.put(1, 2);
        System.out.println("old2 = " + old2);


        sleepSeconds(100);
    }

    @Test
    public void smokeGet() {
        Config config = getConfig();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "1");
        config.setProperty(ClusterProperty.PARTITION_OPERATION_THREAD_COUNT.getName(), "1");

        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(new MapStoreAdapter<Integer, Integer>() {

            @Override
            public Integer load(Integer key) {
                sleepSeconds(20);
                return 11;
            }
        });
        String slowMapName = "slowMap";
        config.getMapConfig(slowMapName).setMapStoreConfig(mapStoreConfig);

        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Object, Object> slowMap = node.getMap(slowMapName);

        spawn(new Runnable() {
            @Override
            public void run() {
                IMap<Object, Object> fast = node.getMap("fast");
                for (int i = 0; i < 10; i++) {
                    fast.set(i, i);
                }

                System.out.println("===> fast size = " + fast.size());
            }
        });

        Object get = slowMap.get(1);
        System.out.println("==> get = " + get);


        sleepSeconds(100);
    }


    @Test
    public void name() {
        Config config = getConfig();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "1");
        config.setProperty(ClusterProperty.PARTITION_OPERATION_THREAD_COUNT.getName(), "1");

        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(new MapStoreAdapter<Integer, Integer>() {

            ConcurrentHashMap store = new ConcurrentHashMap<>();

            @Override
            public Integer load(Integer key) {
                sleepSeconds(10);
                return (Integer) store.get(key);
            }

            @Override
            public void store(Integer key, Integer value) {
                sleepSeconds(10);
                store.put(key, value);
            }

            @Override
            public Map<Integer, Integer> loadAll(Collection<Integer> keys) {
                return null;
            }

            @Override
            public Iterable<Integer> loadAllKeys() {
                return null;
            }
        });
        String slowMapName = "slowMap";
        config.getMapConfig(slowMapName).setMapStoreConfig(mapStoreConfig);

        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Object, Object> fastMap = node.getMap("fast");
        for (int i = 0; i < 10; i++) {
            fastMap.set(i, i);
        }

        spawn(() -> {
            while (true) {
                sleepSeconds(3);
                for (int i = 0; i < 10; i++) {
                    Object res = fastMap.get(i);
                    System.out.println("fast map res = " + res);

                }
            }
        });

        IMap<Object, Object> slowMap = node.getMap(slowMapName);
        Object old = slowMap.put(11, 1);
        System.out.println("slow map old value is=" + old);
        Object old2 = slowMap.put(11, 2);
        System.out.println("slow map old2 value is=" + old2);

        sleepSeconds(100);
    }


    @Test
    public void name3() {
        Config config = getConfig();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "1");
        config.setProperty(ClusterProperty.PARTITION_OPERATION_THREAD_COUNT.getName(), "1");

        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(new MapStoreAdapter<Integer, Integer>() {

            ConcurrentHashMap store = new ConcurrentHashMap<>();

            @Override
            public Integer load(Integer key) {
                sleepSeconds(2);
                return (Integer) store.get(key);
            }

            @Override
            public void store(Integer key, Integer value) {
                sleepSeconds(2);
                store.put(key, value);
            }

            @Override
            public Map<Integer, Integer> loadAll(Collection<Integer> keys) {
                return null;
            }

            @Override
            public Iterable<Integer> loadAllKeys() {
                return null;
            }
        });
        String slowMapName = "slowMap";
        config.getMapConfig(slowMapName).setMapStoreConfig(mapStoreConfig);

        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Object, Object> slowMap = node.getMap(slowMapName);

        spawn(() -> {
            slowMap.set(1, 1);
            System.out.println("slow set i = " + 1);
        });

        spawn(() -> {
            slowMap.set(2, 1);
            System.out.println("slow set i = " + 2);
        });


        sleepSeconds(100);
    }


}