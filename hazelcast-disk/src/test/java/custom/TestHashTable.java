package custom;

import com.hazelcast.disk.core.HashTable;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;

/**
 * @author: ahmetmircik
 * Date: 1/1/14
 */
public class TestHashTable extends AbstractTest {

//    private static ConcurrentMap<Data, Data> mapData = new ConcurrentHashMap<Data, Data>();

    public static void main(String[] args) throws IOException {
        test();

    }
//count-->50000000 write avg--> 3241

    public static void test() throws IOException {
        final String path = getDirName();
        System.out.println("Filename ---> " + path);
        final HashTable hashTable = new HashTable(path);
        long wDiff = 0;
        final int size = 10000000;
                //= 1000000 * 50;
        for (int i = 0; i < size; i++) {
            final Data key = getKey();
            final Data value = getValue();
            long l1 = System.nanoTime();
            hashTable.put(key, value);
            wDiff += System.nanoTime() - l1;

        }
        System.out.println("count-->" + size + " write avg--> " + (wDiff / size));

        hashTable.close();

//        final ReadIndexFile readIndexFile = new ReadIndexFile("-1480649617");
//
//         readIndexFile.close();
    }
}
