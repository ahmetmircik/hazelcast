package com.hazelcast.disk;

import com.hazelcast.disk.core.Hashtable;
import com.hazelcast.disk.helper.FileHelper;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import custom.AbstractDiskTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

/**
 * @author: ahmetmircik
 * Date: 1/14/14
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class PerformanceTest extends AbstractDiskTest {

    @Test
    public void averageWriteTime() throws IOException {

        Hashtable hashtable = null;
        try {
            final String path = getDirName();
            hashtable = new Hashtable(path);
            FileHelper.deleteOnExit(path);

            final int writeCount = 1000000;
            long avg = 0;
            for (int i = 0; i < writeCount; i++) {
                final Data key = getData(8, 20);
                final Data value = getData(8, 100);
                final long start = System.nanoTime();
                hashtable.put(key, value);
                avg += System.nanoTime() - start;
            }
            System.out.println( "Avg write time ---> " + (double) (avg / writeCount) + " nanos");
        } finally {
            hashtable.close();

        }
    }


    @Test
    public void averageLoadAllTime() throws IOException {

        final int writeCount = 1000000;
        Hashtable hashtable = null;
        final String path = getDirName();
        try {
            hashtable = new Hashtable(path);
            for (int i = 0; i < writeCount; i++) {
                final Data key = getData(8, 20);
                final Data value = getData(8, 100);
                hashtable.put(key, value);
            }
        } finally {
            hashtable.close();

        }
        try {
            long total = 0;
            hashtable = new Hashtable(path);
            FileHelper.deleteOnExit(path);
            final long start = System.nanoTime();
            hashtable.loadAll();
            total += System.nanoTime() - start;
            System.out.println("Load all time for " +writeCount + " KVP K{8,20} V{8,100} " + total + " nanos");
        } finally {
            hashtable.close();

        }
     }

}
