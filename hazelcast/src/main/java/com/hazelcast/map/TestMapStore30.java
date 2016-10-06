package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

/**
 * A test to analyze the long term behavior of an exception thrown during deserialization when trying to write the entry
 * to persistent storage.
 */
public class TestMapStore30 {
    private ILogger logger = Logger.getLogger(getClass());

    private static final String MAP_NAME = "testMap" + TestMapStore30.class.getSimpleName();

    private static final int WRITE_DELAY = 2;

    private static final long MAX_TIME_NO_STORE = 10 * 60 * 1000; // 10 minutes

    private static final long MAX_TIME_TEST_RUNNING = 5 * 24 * 60 * 60 * 1000; // 5 days

    public static void main(String[] args) throws Exception {
        new TestMapStore30().testPersistentMap();
    }

    public void testPersistentMap() throws Exception {

        final long initialTime = System.currentTimeMillis();

        // create hazelcast config
        Config config = new XmlConfigBuilder().build();

        // disable multicast for faster startup
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);

        // create map store
        CountingMapStore<FailableTestValue> store = new CountingMapStore<FailableTestValue>();

        // create map store config
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER);
        mapStoreConfig.setWriteDelaySeconds(WRITE_DELAY);
        mapStoreConfig.setClassName(null);
        mapStoreConfig.setImplementation(store);

        // configure map store
        MapConfig mapConfig = config.getMapConfig(MAP_NAME);
        mapConfig.setMapStoreConfig(mapStoreConfig);

        // start hazelcast instance
        HazelcastInstance hcInstance = Hazelcast.newHazelcastInstance(config);

        // try-finally to ensure hc shutdown
        try {
            int index = 0;

            // init map
            IMap<String, FailableTestValue> map = hcInstance.getMap(MAP_NAME);
            logger.info("Size = " + map.size());

            // create test data
            FailableTestValue failableTestValue = new FailableTestValue("should fail");
            failableTestValue.setFailInDeserialize(true);

            // put the first value which will throw de-serialization exceptions
            map.put("key" + index, failableTestValue);

            // variables
            int lastCountNumberStore = store.countNumberStore.get();
            long now = System.currentTimeMillis();
            long lastTimeCountNumberStoreChanged = now;

            // stop the test if no store operation was executed within 10 minutes
            while (now - lastTimeCountNumberStoreChanged < MAX_TIME_NO_STORE || now - initialTime < MAX_TIME_TEST_RUNNING) {
                index++;
                now = System.currentTimeMillis();

                // put other values which should be stored correctly
                FailableTestValue nonFailableTestValue = new FailableTestValue("should not fail");
                map.put("key" + index, nonFailableTestValue);

                store.printCounts((now - initialTime) + "ms after init");

                long timeDiff = now - lastTimeCountNumberStoreChanged;
                int currentCountNumberStore = store.countNumberStore.get();

                // check if the number of stored entries increased
                if (currentCountNumberStore > lastCountNumberStore) {
                    logger.info(String.format("Count number store has increased in the last %d ms", timeDiff));
                    lastTimeCountNumberStoreChanged = now;
                    lastCountNumberStore = currentCountNumberStore;
                } else {
                    logger.info(String.format("Count number store didn't increase for %d ms", timeDiff));
                }

                Thread.sleep(60 * 1000);
            }

            // fail if no store operations executed anymore
            if (now - lastTimeCountNumberStoreChanged > MAX_TIME_NO_STORE) {
                logger.severe(String.format("no store operation executed in the last %d minutes.", MAX_TIME_NO_STORE));
            }

        } finally {
            // shutdown hazelcast instance
            hcInstance.getLifecycleService().terminate();
        }
    }

}
