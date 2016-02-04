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

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.instance.Node;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.RecordStore;
import com.hazelcast.map.impl.mapstore.MapDataStore;
import com.hazelcast.map.impl.mapstore.writebehind.WriteBehindStore;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.Clock;

import java.util.Date;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class TestMapStore {

    private static final int KEY_COUNT = 1000;

    public static void main(String[] args) {
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        MapStoreWithCounter mapStore = new MapStoreWithCounter<Integer, String>();
        mapStoreConfig.setImplementation(mapStore).setWriteDelaySeconds(1);

        Config config = new Config();

        MaxSizeConfig maxSizeConfig = new MaxSizeConfig();
        maxSizeConfig.setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.PER_NODE).setSize(100);

        MapConfig mapConfig = config.getMapConfig("test");
        mapConfig.setEvictionPolicy(EvictionPolicy.LFU).setMaxSizeConfig(maxSizeConfig);
        mapConfig.setBackupCount(1).setMapStoreConfig(mapStoreConfig);

        HazelcastInstance member1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance member2 = Hazelcast.newHazelcastInstance(config);

        IMap<String, String> map = member1.getMap("test");

        new PutThread(map).start();
        new PutTransientThread(map).start();
        new PutAllThread(map).start();
        new RemoveThread(map).start();


        while (true) {
            int storeCount = mapStore.countStore.get();
            int deleteCount = mapStore.countDelete.get();

            System.out.println(new Date(Clock.currentTimeMillis()) + ", wbq size = " + (writeBehindQueueSize(member1, "test")
                    + writeBehindQueueSize(member2, "test")) + ", storeCount = [" + storeCount + "], "
                    + "deleteCount = [" + deleteCount + "]");
            sleepMillis(TimeUnit.SECONDS.toMillis(10));
        }
    }

    public static int writeBehindQueueSize(HazelcastInstance node, String mapName) {
        int size = 0;
        final NodeEngineImpl nodeEngine = getNode(node).getNodeEngine();
        MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);
        final MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        final int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        for (int i = 0; i < partitionCount; i++) {
            final RecordStore recordStore = mapServiceContext.getExistingRecordStore(i, mapName);
            if (recordStore == null) {
                continue;
            }
            final MapDataStore<Data, Object> mapDataStore
                    = recordStore.getMapDataStore();
            size += ((WriteBehindStore) mapDataStore).getWriteBehindQueue().size();
        }
        return size;
    }

    public static Node getNode(HazelcastInstance hz) {
        HazelcastInstanceImpl impl = getHazelcastInstanceImpl(hz);
        return impl != null ? impl.node : null;
    }

    public static HazelcastInstanceImpl getHazelcastInstanceImpl(HazelcastInstance hz) {
        HazelcastInstanceImpl impl = null;
        if (hz instanceof HazelcastInstanceProxy) {
            impl = ((HazelcastInstanceProxy) hz).original;
        } else if (hz instanceof HazelcastInstanceImpl) {
            impl = (HazelcastInstanceImpl) hz;
        }
        return impl;
    }

    static class PutThread extends Thread {

        private Random random = new Random();
        private final IMap<String, String> imap;

        PutThread(IMap<String, String> imap) {
            this.imap = imap;
        }

        @Override
        public void run() {
            while (true) {
                int key = random.nextInt(KEY_COUNT);

                imap.put(String.valueOf(key), String.valueOf(key));

                sleepMillis(1);

            }
        }

    }

    protected static void sleepMillis(long millis) {
        try {
            MILLISECONDS.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    static class PutAllThread extends Thread {

        private Random random = new Random();
        private final IMap<String, String> imap;

        PutAllThread(IMap<String, String> imap) {
            this.imap = imap;
        }

        @Override
        public void run() {
            while (true) {

                HashMap<String, String> batch = new HashMap<String, String>();
                for (int i = 0; i < 1000; i++) {
                    int key = random.nextInt(KEY_COUNT);
                    batch.put(String.valueOf(key), String.valueOf(key));
                }

                imap.putAll(batch);

                sleepMillis(10);

            }
        }

    }


    static class RemoveThread extends Thread {
        private Random random = new Random();
        private final IMap<String, String> imap;

        RemoveThread(IMap<String, String> imap) {
            this.imap = imap;
        }

        @Override
        public void run() {
            while (true) {
                int key = random.nextInt(KEY_COUNT);

                imap.remove(String.valueOf(key));

                sleepMillis(100);

            }
        }
    }


    static class PutTransientThread extends Thread {

        private Random random = new Random();
        private final IMap<String, String> imap;

        PutTransientThread(IMap<String, String> imap) {
            this.imap = imap;
        }

        @Override
        public void run() {
            while (true) {
                int key = random.nextInt(KEY_COUNT);

                imap.putTransient(String.valueOf(key), String.valueOf(key), 100, MILLISECONDS);

                sleepMillis(1);

            }
        }
    }
}
