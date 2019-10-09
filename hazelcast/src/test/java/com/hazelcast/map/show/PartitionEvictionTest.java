package com.hazelcast.map.show;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryEvictedListener;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.map.impl.eviction.EvictorImpl.NEW_SAMPLING;
import static com.hazelcast.map.impl.eviction.EvictorImpl.NEW_SAMPLING_POOL_SIZE;
import static com.hazelcast.map.impl.eviction.EvictorImpl.SAMPLE_COUNT;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PartitionEvictionTest extends HazelcastTestSupport {

    public static final int MAX_ALLOWED_SIZE = 10_000;
    public static final int PARTITION_COUNT = 1;
    public static final String MAP_NAME = "map";

    @Test
    public void testOldSampling() {
        Config config = new Config();

        config.setProperty(NEW_SAMPLING.getName(), "false");
        config.setProperty(SAMPLE_COUNT.getName(), "15");

        runTestWith(config, false);
    }

    @Test
    public void testNewSampling() {
        Config config = new Config();

        config.setProperty(NEW_SAMPLING.getName(), "true");
        config.setProperty(SAMPLE_COUNT.getName(), "6");
        config.setProperty(NEW_SAMPLING_POOL_SIZE.getName(), "12");

        runTestWith(config, true);
    }

    private void runTestWith(Config config, boolean newSampling) {
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), String.valueOf(PARTITION_COUNT));
        MapConfig mapConfig = config.getMapConfig(MAP_NAME);
        MaxSizeConfig maxSizeConfig = mapConfig.setEvictionPolicy(EvictionPolicy.LRU).getMaxSizeConfig();
        maxSizeConfig.setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.PER_PARTITION);

        maxSizeConfig.setSize(MAX_ALLOWED_SIZE);

        final ConcurrentLinkedQueue<Integer> evictedList = new ConcurrentLinkedQueue<>();
        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Integer, Integer> map = node.getMap(MAP_NAME);
        map.addEntryListener((EntryEvictedListener<Integer, Integer>)
                event -> evictedList.add(event.getKey()), false);

        for (int i = 0; i < MAX_ALLOWED_SIZE; i++) {
            map.set(i, i);
        }

        sleepSeconds(1);

        // Use half of the entries
        for (int i = MAX_ALLOWED_SIZE / 2; i < MAX_ALLOWED_SIZE; i++) {
            map.get(i);
        }

        // Add half number of existing entries to exceed evictable size.
        for (int i = MAX_ALLOWED_SIZE; i < MAX_ALLOWED_SIZE + MAX_ALLOWED_SIZE / 2; i++) {
            map.set(i, i);
        }

        // Expect evicted events
        while (evictedList.size() < MAX_ALLOWED_SIZE / 2) {
            LockSupport.parkNanos(1);
        }

        int falsePositives = 0;
        int correct = 0;
        for (Integer key : evictedList) {
            if (key >= 0 && key < MAX_ALLOWED_SIZE / 2) {
                correct++;
            } else {
                falsePositives++;
            }
        }

        System.err.println((newSampling ? "NEW: " : "OLD: ") + " map-size: "
                + map.size() + ", falsePositives="
                + falsePositives + ", correct=" + correct);
    }
}
