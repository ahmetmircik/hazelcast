package com.hazelcast.disk;

import com.hazelcast.disk.core.Hashtable;
import com.hazelcast.disk.helper.FileHelper;
import com.hazelcast.disk.helper.RecordHelper;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import custom.AbstractDiskTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author: ahmetmircik
 * Date: 1/14/14
 */

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class BasicHashtableTest extends AbstractDiskTest {

    private static List<Object[]> DATA_LIST;
    private static Hashtable<Data, Data> hashtable;
    private static int writeCount;

    @BeforeClass
    public static void before() {
        final String path = getDirName();
        hashtable = getHashTable(path);
        FileHelper.deleteOnExit(path);
        writeCount = 100000;
        DATA_LIST = new ArrayList<Object[]>(writeCount);
    }


    @Test
    public void test_1_HashTableWrite() {
        for (int i = 0; i < writeCount; i++) {
            final Data key = getData(8, 100);
            final Data value = getData(8, 200);
            DATA_LIST.add(new Data[]{key, value});
            hashtable.put(key, value);
        }
        Assert.assertEquals(writeCount, hashtable.size());
    }

    @Test
    public void test_2_Get() throws IOException {
        int falseCount = 0;
        for (final Object[] e : DATA_LIST) {
            final byte[] expected = RecordHelper.asByteArray(e[1]);
            final byte[] actual = RecordHelper.asByteArray(hashtable.get((Data) e[0]));
            Assert.assertTrue(Arrays.equals(expected, actual));

//            if (!Arrays.equals(expected,actual))
//            {
//                falseCount++;
////                final byte[] xx = hashtable.get(e[0]).getBuffer();
//            }
        }

//        System.err.println("------------> test_2_Get false count\t:" + falseCount);

    }

    @Test
    public void test_3_LoadAll() throws IOException {
        DATA_LIST.clear();
        final List<Object[]> list = hashtable.loadAll();
        DATA_LIST.addAll(list);
        Assert.assertEquals(writeCount, DATA_LIST.size());
    }

    @Test
    public void test_4_CompareReadDataWithWritten() {

        Assert.assertEquals(writeCount, DATA_LIST.size());
        for (final Object[] e : DATA_LIST) {
            final Data value = hashtable.get((Data) e[0]);
//            if (!Arrays.equals(e[1].getBuffer(), value.getBuffer())) {
//                falseCount++;
//            }
            Assert.assertTrue(Arrays.equals(RecordHelper.asByteArray(e[1]), value.getBuffer()));
        }
        //    System.err.println("------------> test_3_CompareReadDataWithWritten false count\t:" + falseCount);
    }

    @Test
    public void test_5_RemoveOneByOne() {

        Assert.assertEquals(writeCount, DATA_LIST.size());

        for (final Object[] e : DATA_LIST) {
            final Data value = hashtable.remove((Data) e[0]);
//            if (!Arrays.equals(e[1].getBuffer(), value.getBuffer())) {
//                falseCount++;
//            }
            Assert.assertTrue(Arrays.equals(RecordHelper.asByteArray(e[1]), value.getBuffer()));
        }

        Assert.assertEquals(0, hashtable.size());
        //    System.err.println("------------> test_3_CompareReadDataWithWritten false count\t:" + falseCount);
    }

    @AfterClass
    public static void after() throws IOException {
        hashtable.close();
    }

}