package com.hazelcast.disk;

import com.hazelcast.disk.core.HashTable;
import com.hazelcast.disk.helper.FileHelper;
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
public class TestHashTable extends AbstractDiskTest {

    private static List<Data[]> DATA_LIST;
    private static HashTable hashTable;
    private static int writeCount;

    @BeforeClass
    public static void before() {
        final String path = getDirName();
        hashTable = new HashTable(path);
        writeCount = 1500000;
        DATA_LIST = new ArrayList<Data[]>(writeCount);
        FileHelper.deleteOnExit(path);
    }


    @Test
    public void test_1_HashTableWrite() {
        for (int i = 0; i < writeCount; i++) {
            final Data key = getData(8, 100);
            final Data value = getData(8, 200);
            DATA_LIST.add(new Data[]{key, value});
            hashTable.put(key, value);
        }
        Assert.assertEquals(writeCount, hashTable.size());
    }

    @Test
    public void test_2_Get() throws IOException {
        int falseCount = 0;
        for (final Data[] e : DATA_LIST) {
            final byte[] expected = e[1].getBuffer();
            final byte[] actual = hashTable.get(e[0]).getBuffer();
            Assert.assertTrue(Arrays.equals(expected, actual));

//            if (!Arrays.equals(expected,actual))
//            {
//                falseCount++;
////                final byte[] xx = hashTable.get(e[0]).getBuffer();
//            }
        }

//        System.err.println("------------> test_2_Get false count\t:" + falseCount);

    }

    @Test
    public void test_3_LoadAll() throws IOException {
        DATA_LIST.clear();
        final List<Data[]> list = hashTable.loadAll();
        DATA_LIST.addAll(list);
        Assert.assertEquals(writeCount, DATA_LIST.size());
    }

    @Test
    public void test_4_CompareReadDataWithWritten() {

        Assert.assertEquals(writeCount, DATA_LIST.size());
        for (final Data[] e : DATA_LIST) {
            final Data value = hashTable.get(e[0]);
//            if (!Arrays.equals(e[1].getBuffer(), value.getBuffer())) {
//                falseCount++;
//            }
            Assert.assertTrue(Arrays.equals(e[1].getBuffer(), value.getBuffer()));
        }
        //    System.err.println("------------> test_3_CompareReadDataWithWritten false count\t:" + falseCount);
    }

    @Test
    public void test_5_RemoveOneByOne() {

        Assert.assertEquals(writeCount, DATA_LIST.size());

        for (final Data[] e : DATA_LIST) {
            final Data value = hashTable.remove(e[0]);
//            if (!Arrays.equals(e[1].getBuffer(), value.getBuffer())) {
//                falseCount++;
//            }
            Assert.assertTrue(Arrays.equals(e[1].getBuffer(), value.getBuffer()));
        }
        //    System.err.println("------------> test_3_CompareReadDataWithWritten false count\t:" + falseCount);
    }

    @AfterClass
    public static void after() throws IOException {
        hashTable.close();
    }

}