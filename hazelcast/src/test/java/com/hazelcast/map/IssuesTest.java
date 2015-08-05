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
import com.hazelcast.config.GlobalSerializerConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class IssuesTest extends HazelcastTestSupport {

    @Test
    public void testIssue321_1() throws Exception {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<Integer, Integer> map = instance.getMap(randomString());
        final BlockingQueue<com.hazelcast.core.EntryEvent<Integer, Integer>> events1 = new LinkedBlockingQueue<com.hazelcast.core.EntryEvent<Integer, Integer>>();
        final BlockingQueue<com.hazelcast.core.EntryEvent<Integer, Integer>> events2 = new LinkedBlockingQueue<com.hazelcast.core.EntryEvent<Integer, Integer>>();
        map.addEntryListener(new EntryAdapter<Integer, Integer>() {
            @Override
            public void entryAdded(com.hazelcast.core.EntryEvent<Integer, Integer> event) {
                events2.add(event);
            }
        }, false);
        map.addEntryListener(new EntryAdapter<Integer, Integer>() {
            @Override
            public void entryAdded(com.hazelcast.core.EntryEvent<Integer, Integer> event) {
                events1.add(event);
            }
        }, true);
        map.put(1, 1);
        com.hazelcast.core.EntryEvent<Integer, Integer> event1 = events1.poll(10, TimeUnit.SECONDS);
        com.hazelcast.core.EntryEvent<Integer, Integer> event2 = events2.poll(10, TimeUnit.SECONDS);
        assertNotNull(event1);
        assertNotNull(event2);
        assertNotNull(event1.getValue());
        assertNull(event2.getValue());
    }

    @Test
    public void testIssue321_2() throws Exception {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<Integer, Integer> map = instance.getMap(randomString());
        final BlockingQueue<com.hazelcast.core.EntryEvent<Integer, Integer>> events1 = new LinkedBlockingQueue<com.hazelcast.core.EntryEvent<Integer, Integer>>();
        final BlockingQueue<com.hazelcast.core.EntryEvent<Integer, Integer>> events2 = new LinkedBlockingQueue<com.hazelcast.core.EntryEvent<Integer, Integer>>();
        map.addEntryListener(new EntryAdapter<Integer, Integer>() {
            @Override
            public void entryAdded(com.hazelcast.core.EntryEvent<Integer, Integer> event) {
                events1.add(event);
            }
        }, true);
        Thread.sleep(50L);
        map.addEntryListener(new EntryAdapter<Integer, Integer>() {
            @Override
            public void entryAdded(com.hazelcast.core.EntryEvent<Integer, Integer> event) {
                events2.add(event);
            }
        }, false);
        map.put(1, 1);
        com.hazelcast.core.EntryEvent<Integer, Integer> event1 = events1.poll(10, TimeUnit.SECONDS);
        com.hazelcast.core.EntryEvent<Integer, Integer> event2 = events2.poll(10, TimeUnit.SECONDS);
        assertNotNull(event1);
        assertNotNull(event2);
        assertNotNull(event1.getValue());
        assertNull(event2.getValue());
    }

    @Test
    public void testIssue321_3() throws Exception {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<Integer, Integer> map = instance.getMap(randomString());
        final BlockingQueue<com.hazelcast.core.EntryEvent<Integer, Integer>> events = new LinkedBlockingQueue<com.hazelcast.core.EntryEvent<Integer, Integer>>();
        EntryAdapter<Integer, Integer> listener = new EntryAdapter<Integer, Integer>() {
            @Override
            public void entryAdded(com.hazelcast.core.EntryEvent<Integer, Integer> event) {
                events.add(event);
            }
        };
        map.addEntryListener(listener, true);
        Thread.sleep(50L);
        map.addEntryListener(listener, false);
        map.put(1, 1);
        com.hazelcast.core.EntryEvent<Integer, Integer> event1 = events.poll(10, TimeUnit.SECONDS);
        com.hazelcast.core.EntryEvent<Integer, Integer> event2 = events.poll(10, TimeUnit.SECONDS);
        assertNotNull(event1);
        assertNotNull(event2);
        assertNotNull(event1.getValue());
        assertNull(event2.getValue());
    }

    @Test
    public void testIssue304() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<String, String> map = instance.getMap(randomString());
        map.lock("1");
        assertEquals(0, map.size());
        assertEquals(0, map.entrySet().size());
        map.put("1", "value");
        assertEquals(1, map.size());
        assertEquals(1, map.entrySet().size());
        map.unlock("1");
        assertEquals(1, map.size());
        assertEquals(1, map.entrySet().size());
        map.remove("1");
        assertEquals(0, map.size());
        assertEquals(0, map.entrySet().size());
    }

    /*
       github issue 174
    */
    @Test
    public void testIssue174NearCacheContainsKeySingleNode() {
        String name = randomString();
        Config config = getConfig();
        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        config.getMapConfig(name).setNearCacheConfig(nearCacheConfig);

        HazelcastInstance instance = createHazelcastInstance(config);
        IMap<String, String> map = instance.getMap(name);
        map.put("key", "value");
        assertTrue(map.containsKey("key"));
    }

    @Test
    public void testIssue1067GlobalSerializer() {
        String name = randomString();
        Config config = getConfig();
        config.getSerializationConfig().setGlobalSerializerConfig(new GlobalSerializerConfig()
                .setImplementation(new DummyStreamSerializer()));
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        IMap<Object, Object> map = instance.getMap(name);
        for (int i = 0; i < 10; i++) {
            map.put(i, new DummyValue());
        }
        assertEquals(10, map.size());

        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        IMap<Object, Object> map2 = hz2.getMap(name);
        assertEquals(10, map2.size());
        assertEquals(10, map.size());

        for (int i = 0; i < 10; i++) {
            Object o = map2.get(i);
            assertNotNull(o);
            assertTrue(o instanceof DummyValue);
        }
    }

    @Test
    public void testMapInterceptorInstanceAware() {
        String name = randomString();
        Config config = getConfig();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        IMap<Object, Object> map = instance1.getMap(name);

        InstanceAwareMapInterceptorImpl interceptor = new InstanceAwareMapInterceptorImpl();
        map.addInterceptor(interceptor);
        assertNotNull(interceptor.hazelcastInstance);
        assertEquals(instance1, interceptor.hazelcastInstance);

        for (int i = 0; i < 100; i++) {
            map.put(i, i);
        }

        for (int i = 0; i < 100; i++) {
            assertEquals("notNull", map.get(i));
        }
    }

    @Test // Issue #1795
    public void testMapClearDoesNotTriggerEqualsOrHashCodeOnKeyObject() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap map = instance.getMap(randomString());
        CompositeKey key = new CompositeKey();
        map.put(key, "value");
        map.clear();
        assertFalse("hashCode method should not have been called on key during clear", CompositeKey.hashCodeCalled);
        assertFalse("equals method should not have been called on key during clear", CompositeKey.equalsCalled);
    }

    private static class DummyValue {
    }

    static class InstanceAwareMapInterceptorImpl implements MapInterceptor, HazelcastInstanceAware {

        transient HazelcastInstance hazelcastInstance;

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hazelcastInstance = hazelcastInstance;
        }

        @Override
        public Object interceptGet(Object value) {
            return null;
        }

        @Override
        public void afterGet(Object value) {

        }

        @Override
        public Object interceptPut(Object oldValue, Object newValue) {
            if (hazelcastInstance != null) {
                return "notNull";
            }
            return ">null";
        }

        @Override
        public void afterPut(Object value) {

        }

        @Override
        public Object interceptRemove(Object removedValue) {
            return null;
        }

        @Override
        public void afterRemove(Object value) {

        }
    }

    public static class CompositeKey implements Serializable {
        static boolean hashCodeCalled = false;
        static boolean equalsCalled = false;

        @Override
        public int hashCode() {
            hashCodeCalled = true;
            return super.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            equalsCalled = true;
            return super.equals(o);
        }
    }

    private static class DummyStreamSerializer implements StreamSerializer {
        public void write(ObjectDataOutput out, Object object) throws IOException {
        }

        public Object read(ObjectDataInput in) throws IOException {
            return new DummyValue();
        }

        public int getTypeId() {
            return 123;
        }

        public void destroy() {
        }
    }
}
