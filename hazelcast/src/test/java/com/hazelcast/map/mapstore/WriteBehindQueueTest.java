package com.hazelcast.map.mapstore;

import com.hazelcast.map.writebehind.DelayedEntry;
import com.hazelcast.map.writebehind.WriteBehindQueue;
import com.hazelcast.map.writebehind.WriteBehindQueues;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class WriteBehindQueueTest extends HazelcastTestSupport {

    @Test
    public void smoke() {
        final WriteBehindQueue<DelayedEntry> queue = WriteBehindQueues.createDefaultWriteBehindQueue(true);

        assertEquals(0, queue.size());
    }

    @Test
    public void testOffer() {
        final WriteBehindQueue<DelayedEntry> queue = WriteBehindQueues.createDefaultWriteBehindQueue(true);

        fillQueue(queue, 1000);

        assertEquals(1000, queue.size());
    }

    @Test
    public void testRemoveEmpty() {
        final WriteBehindQueue<DelayedEntry> queue = WriteBehindQueues.createDefaultWriteBehindQueue(true);

        queue.removeFirst();

        assertEquals(0, queue.size());
    }

    @Test
    public void testClear() {
        final WriteBehindQueue<DelayedEntry> queue = WriteBehindQueues.createDefaultWriteBehindQueue(true);

        queue.clear();

        assertEquals(0, queue.size());
    }


    @Test
    public void testClearFull() {
        final WriteBehindQueue<DelayedEntry> queue = WriteBehindQueues.createDefaultWriteBehindQueue(true);

        fillQueue(queue, 1000);

        queue.clear();

        assertEquals(0, queue.size());
    }


    @Test
    public void testRemoveAll() {
        final WriteBehindQueue<DelayedEntry> queue = WriteBehindQueues.createDefaultWriteBehindQueue(true);

        fillQueue(queue, 1000);

        for (int i = 0; i < 1000; i++) {
            queue.removeFirst();
        }

        assertEquals(0, queue.size());
    }

    private void fillQueue(WriteBehindQueue queue, int numberOfItems) {
        for (int i = 0; i < numberOfItems; i++) {
            final DelayedEntry<Object, Object> e = DelayedEntry.createEmpty();
            queue.offer(e);
        }

    }

}
