package com.hazelcast.map.show;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryEvictedListener;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.LockSupport;

public class PartitionEvictionTest extends HazelcastTestSupport {

    @Test
    public void test() {
        String evictableMap = "evictable";

        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        MapConfig mapConfig = config.getMapConfig(evictableMap);

        MaxSizeConfig maxSizeConfig = mapConfig.setEvictionPolicy(EvictionPolicy.LRU).getMaxSizeConfig();
        maxSizeConfig.setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.PER_PARTITION);
        maxSizeConfig.setSize(1000);

        final ConcurrentLinkedQueue<Integer> evictedList = new ConcurrentLinkedQueue<Integer>();
        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Integer, Integer> map = node.getMap(evictableMap);
        map.addEntryListener((EntryEvictedListener<Integer, Integer>)
                event -> evictedList.add(event.getKey()), false);

        for (int i = 0; i < 1000; i++) {
            map.set(i, i);
        }

        sleepAtLeastSeconds(1);

        for (int i = 500; i < 1000; i++) {
            map.get(i);
        }

        sleepAtLeastSeconds(1);

        for (int i = 1000; i < 1500; i++) {
            map.set(i, i);
        }

        while (evictedList.size() < 500) {
            LockSupport.parkNanos(1);
        }

        int falsePositives = 0;
        int correct = 0;
        for (Integer key : evictedList) {
            if (key >= 0 && key < 500) {
                correct++;
            } else {
                falsePositives++;
            }
        }

        System.err.println(map.size() + ", falsePositives="
                + falsePositives + ", correct=" + correct);
    }

    @Test
    public void test_10_000() {
        int keyCount = 100000;
        String evictableMap = "evictable";

        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        MapConfig mapConfig = config.getMapConfig(evictableMap);

        MaxSizeConfig maxSizeConfig = mapConfig.setEvictionPolicy(EvictionPolicy.LRU).getMaxSizeConfig();
        maxSizeConfig.setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.PER_PARTITION);
        maxSizeConfig.setSize(keyCount);

        final ConcurrentLinkedQueue<Integer> evictedList = new ConcurrentLinkedQueue<Integer>();
        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Integer, Integer> map = node.getMap(evictableMap);
        map.addEntryListener((EntryEvictedListener<Integer, Integer>) event -> evictedList.add(event.getKey()), true);


        for (int i = 0; i < keyCount; i++) {
            map.set(i, i);
        }

        sleepAtLeastSeconds(1);

        for (int i = keyCount / 2; i < keyCount; i++) {
            map.get(i);
        }

        sleepAtLeastSeconds(1);

        for (int i = keyCount; i < keyCount + keyCount / 2; i++) {
            map.set(i, i);
        }

        while (evictedList.size() < keyCount / 2) {
            LockSupport.parkNanos(1);
        }

        int falsePositives = 0;
        int correct = 0;
        for (Integer key : evictedList) {
            if (key >= 0 && key < keyCount / 2) {
                correct++;
            } else {
                falsePositives++;
            }
        }

        System.err.println(map.size() + ", falsePositives="
                + falsePositives + ", correct=" + correct);
    }
}
