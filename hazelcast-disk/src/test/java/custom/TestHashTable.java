package custom;

import com.hazelcast.disk.core.HashTable2;
import com.hazelcast.disk.core.Hasher;
import com.hazelcast.disk.core.ReadIndexFile;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author: ahmetmircik
 * Date: 1/1/14
 */
public class TestHashTable extends AbstractTest {

    private static ConcurrentMap<Data, Data> mapData = new ConcurrentHashMap<Data, Data>();

    private static final Hasher<Data, Integer> HASHER = Hasher.DATA_HASHER;


    public static void main(String[] args) throws IOException {

        final String path = getDirName();
        test(path);
        read(path);
//        readCompare(path);

    }
//count-->50000000 write avg--> 3241

    public static void test(String path) throws IOException {

        final HashTable2 hashTable = new HashTable2(path);
        long wDiff = 0;
        final int size = 120;
        for (int i = 0; i < size; i++) {
            final Data key = getKey(16);
            final Data value = getValue(512);
            long l1 = System.nanoTime();
            hashTable.put(key, value);
            wDiff += System.nanoTime() - l1;
            mapData.put(key, value);
        }


        System.out.println("count-->" + size + " write avg--> " + (wDiff / size));

        hashTable.close();
        System.out.println("closed");
    }

    public static void readCompare(String path) {
        final ReadIndexFile readIndexFile = new ReadIndexFile(path);
        long avgCounter = 0;
        for (Map.Entry<Data, Data> entry : mapData.entrySet()) {
            long start = System.nanoTime();
            final Data data = readIndexFile.getData(entry.getKey());
            final Data value = entry.getValue();
            try {
                if (!value.equals(data)) {
                    throw new RuntimeException("should be equal...");
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
            avgCounter += (System.nanoTime() - start);

        }
        System.out.println("count-->" + mapData.size() + " read avg--> " + (avgCounter / mapData.size()));

        readIndexFile.close();

    }

    public static void read(String path) throws IOException {

        long l1=0 ;
        final ReadIndexFile readIndexFile = new ReadIndexFile(path);
        final Map<Data, Data> dataDataMap = readIndexFile.readSequentially();
        for (Map.Entry<Data, Data> entry : mapData.entrySet()) {
            long start = System.nanoTime();
            final Data data = dataDataMap.get(entry.getKey());
            long diff =System.nanoTime()-start;
            l1+=diff;

            final Data value = entry.getValue();
            try {
                if (!value.equals(data)) {
                    throw new RuntimeException("should be equal...");
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

        }
        System.out.println("key count " + readIndexFile.keyCount);

        final int count = readIndexFile.getCount();
        final long l = l1 / count;
        System.out.println("count-->" + count + " read avg--> " + l);


        readIndexFile.close();
    }
}
