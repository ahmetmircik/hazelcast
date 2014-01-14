package com.hazelcast.disk;

import com.hazelcast.disk.core.Hashtable;
import com.hazelcast.disk.helper.FileHelper;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.Repeat;
import com.hazelcast.test.annotation.SlowTest;
import custom.AbstractDiskTest;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
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
public class RewriteTest extends AbstractDiskTest {

    private static final String PATH = getDirName();
    private static Hashtable<Data,Data> HASHTABLE;
    private static final int WRITE_COUNT = 100000;
    private static final int REPEAT_COUNT = 3;
    private static long resultAccumulator = 0;
    private static int repeatHelper = 0;


    @BeforeClass
    public static void before() {
        FileHelper.deleteOnExit(PATH);
    }


    @Test
    @Repeat(REPEAT_COUNT)
    public void rewriteExistingHashtable() throws IOException {

        try {
            HASHTABLE = getHashTable(PATH);

            for (int i = 0; i < WRITE_COUNT; i++) {
                final Data key = getData(8, 20);
                final Data value = getData(8, 100);
                HASHTABLE.put(key, value);
            }
        } finally {
            repeatHelper ++;
            if(repeatHelper == REPEAT_COUNT){
                resultAccumulator = HASHTABLE.size();
            }
            HASHTABLE.close();

        }
    }


    @AfterClass
    public static void after() {
        Assert.assertEquals("Rewrite pocess failed...", 3 * WRITE_COUNT, resultAccumulator);
        FileHelper.deleteOnExit(PATH);
    }
}
