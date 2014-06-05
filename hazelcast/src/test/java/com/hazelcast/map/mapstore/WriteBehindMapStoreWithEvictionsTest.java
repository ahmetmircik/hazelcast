package com.hazelcast.map.mapstore;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapStore;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class WriteBehindMapStoreWithEvictionsTest extends HazelcastTestSupport {

    @Test
    public void testWriteBehind_withEvict() throws Exception {

        final CounterMapStore mapStore = new CounterMapStore<Integer, String>();
        final IMap<Object, Object> map = MapWithMapStoreBuilder.create()
                .withMapStore(mapStore)
                .withNodeCount(1)
                .withBackupCount(0)
                .withWriteDelaySeconds(3)
                .withPartitionCount(1)
                .build();

        final int numberOfItems = 1000;
        for (int i = 0; i < numberOfItems; i++) {
            map.put(i, i);
        }
        sleepMillis(700);
        for (int i = 0; i < numberOfItems; i++) {
            map.evict(i);
        }
        sleepMillis(4000);
        assertEquals(numberOfItems, mapStore.countStore.intValue());

    }

    @Test
    public void testWriteBehind_withEvict_onSameKey() throws Exception {
        final CounterMapStore mapStore = new CounterMapStore<Integer, String>();
        final IMap<Object, Object> map = MapWithMapStoreBuilder.create()
                .withMapStore(mapStore)
                .withNodeCount(1)
                .withBackupCount(0)
                .withWriteDelaySeconds(3)
                .withPartitionCount(1)
                .build();

        final int numberOfItems = 2041;
        for (int i = 1; i <= numberOfItems; i++) {
            map.put(0, i);
        }
        sleepMillis(800);

        map.evict(0);

        sleepMillis(4000);

        assertFinalValueEquals(numberOfItems, (Integer) map.get(0));
    }

    private void assertFinalValueEquals(final int expected, final int actual) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(expected, actual);
            }
        }, 20);
    }


    private static class MapWithMapStoreBuilder<K, V> {

        private HazelcastInstance[] nodes;

        private int nodeCount;

        private int partitionCount = 271;

        private int backupCount = 1;

        private String mapName = randomMapName("default");

        private MapStore<K, V> mapStore;

        private int writeDelaySeconds = 0;

        private MapWithMapStoreBuilder() {
        }

        public static <K, V> MapWithMapStoreBuilder<K, V> create() {
            return new MapWithMapStoreBuilder<K, V>();
        }


        public MapWithMapStoreBuilder<K, V> mapName(String mapName) {
            if (mapName == null) {
                throw new IllegalArgumentException("mapName is null");
            }
            this.mapName = mapName;
            return this;
        }

        public MapWithMapStoreBuilder<K, V> withNodeCount(int nodeCount) {
            if (nodeCount < 1) {
                throw new IllegalArgumentException("nodeCount < 1");
            }
            this.nodeCount = nodeCount;
            return this;
        }

        public MapWithMapStoreBuilder<K, V> withPartitionCount(int partitionCount) {
            if (partitionCount < 1) {
                throw new IllegalArgumentException("partitionCount < 1");
            }
            this.partitionCount = partitionCount;
            return this;
        }

        public MapWithMapStoreBuilder<K, V> withBackupCount(int backupCount) {
            if (backupCount < 0) {
                throw new IllegalArgumentException("backupCount < 1");
            }
            this.backupCount = backupCount;
            return this;
        }


        public MapWithMapStoreBuilder<K, V> withMapStore(MapStore<K, V> mapStore) {
            this.mapStore = mapStore;
            return this;
        }

        public MapWithMapStoreBuilder<K, V> withWriteDelaySeconds(int writeDelaySeconds) {
            if (writeDelaySeconds < 0) {
                throw new IllegalArgumentException("writeDelaySeconds < 0");
            }
            this.writeDelaySeconds = writeDelaySeconds;
            return this;
        }


        public IMap<K, V> build() {
            if (backupCount > nodeCount - 1) {
                throw new IllegalArgumentException("backupCount > nodeCount - 1");
            }
            final MapStoreConfig mapStoreConfig = new MapStoreConfig();
            mapStoreConfig.setImplementation(mapStore).setWriteDelaySeconds(writeDelaySeconds);

            final Config config = new Config();
            config.getMapConfig(mapName)
                    .setBackupCount(backupCount)
                    .setMapStoreConfig(mapStoreConfig);

            config.setProperty(GroupProperties.PROP_PARTITION_COUNT, String.valueOf(partitionCount));
            // nodes.
            final TestHazelcastInstanceFactory instanceFactory = new TestHazelcastInstanceFactory(nodeCount);
            nodes = instanceFactory.newInstances(config);
            return nodes[0].getMap(mapName);
        }
    }


    public static class CounterMapStore<K, V> implements MapStore<K, V> {

        protected final Map<K, V> store = new ConcurrentHashMap();

        protected AtomicInteger countStore = new AtomicInteger(0);

        public CounterMapStore() {
        }

        @Override
        public void store(K key, V value) {
            countStore.incrementAndGet();
            store.put(key, value);
        }

        @Override
        public void storeAll(Map<K, V> map) {
            countStore.addAndGet(map.size());
            for (Map.Entry<K, V> kvp : map.entrySet()) {
                store.put(kvp.getKey(), kvp.getValue());
            }
        }

        @Override
        public void delete(K key) {

        }

        @Override
        public void deleteAll(Collection<K> keys) {

        }

        @Override
        public V load(K key) {
            return store.get(key);
        }

        @Override
        public Map<K, V> loadAll(Collection<K> keys) {
            return null;
        }

        @Override
        public Set<K> loadAllKeys() {
            return null;
        }
    }


}
