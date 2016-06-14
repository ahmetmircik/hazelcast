package com.hazelcast.map.impl.eviction;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.FREE_HEAP_PERCENTAGE;

public class EvictionTest {

    public static void main(String[] args) throws InterruptedException {
        System.out.println("EvictionTest.main");
        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(5));
        final int minFreeHeapPercentage = 12;
        final String mapName = "exampleMap";
        final Config config = createConfig(FREE_HEAP_PERCENTAGE, minFreeHeapPercentage, mapName);

        Integer nodeCount = Integer.getInteger("extra.node.count", 0);
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);

        for (int i = 0; i < nodeCount; i++) {
            Hazelcast.newHazelcastInstance(config);
        }


        IMap<Integer, byte[]> map = hz.getMap(mapName);

        Random random = new Random();
        byte[] value = new byte[1024];
        random.nextBytes(value);

        for (int i = 0; ; i++) {
            map.put(i, value);
        }
    }

    static Config createConfig(MaxSizeConfig.MaxSizePolicy maxSizePolicy, int maxSize, String mapName) {
        Config config = new Config();

        MaxSizeConfig msc = new MaxSizeConfig();
        msc.setMaxSizePolicy(maxSizePolicy);
        msc.setSize(maxSize);

        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.setEvictionPolicy(EvictionPolicy.LRU);
        mapConfig.setEvictionPercentage(10);
        mapConfig.setMaxSizeConfig(msc);
        mapConfig.setMinEvictionCheckMillis(0L);

        return config;
    }


}
