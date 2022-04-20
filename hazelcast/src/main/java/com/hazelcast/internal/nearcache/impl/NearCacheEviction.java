package com.hazelcast.internal.nearcache.impl;/*
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

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.map.impl.nearcache.NearCachedClientMapProxy;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.RandomPicker;
import com.hazelcast.map.IMap;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class NearCacheEviction {
    public static void main(String[] args) throws InterruptedException {
        HazelcastInstance node = Hazelcast.newHazelcastInstance();
        IMap<Object, Object> nodeMap = node.getMap("test");
        for (int i = 0; i < 100_000; i++) {
            nodeMap.set(i, i);
        }

        ClientConfig clientConfig = new ClientConfig();
        NearCacheConfig nearCacheConfig = new NearCacheConfig("test");
        EvictionConfig evictionConfig = nearCacheConfig.getEvictionConfig();
        evictionConfig.setEvictionPolicy(EvictionPolicy.LFU);
        evictionConfig.setMaxSizePolicy(MaxSizePolicy.ENTRY_COUNT);
        evictionConfig.setSize(1000);

        AtomicBoolean running = new AtomicBoolean(true);
        clientConfig.addNearCacheConfig(nearCacheConfig);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        IMap<Object, Object> map = client.getMap("test");

        ExecutorService pool = Executors.newFixedThreadPool(8);
        Runnable get = new Runnable() {
            @Override
            public void run() {
                while (running.get()) {
                    map.get(RandomPicker.getInt(0, 100_000));
                }
            }
        };
        for (int i = 0; i < 4; i++) {
            pool.execute(get);
        }

        Runnable remove = new Runnable() {
            @Override
            public void run() {
                while (running.get()) {
                    map.remove(RandomPicker.getInt(0, 100_000));
                }
            }
        };

        for (int i = 0; i < 4; i++) {
            pool.execute(remove);
        }

        TimeUnit.SECONDS.sleep(10);
        running.set(false);

        pool.shutdown();
        pool.awaitTermination(10, TimeUnit.SECONDS);


        System.err.println(map.getLocalMapStats().getNearCacheStats());
        int real = ((NearCachedClientMapProxy) map).getNearCache().size();
        System.err.println("real size=" + real);
    }
}
