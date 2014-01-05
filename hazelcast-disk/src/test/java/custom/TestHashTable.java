package custom;

import com.hazelcast.disk.core.HashTable;
import com.hazelcast.disk.core.ReadIndexFile;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author: ahmetmircik
 * Date: 1/1/14
 */
public class TestHashTable extends AbstractTest {

    private static ConcurrentMap<Data, Data> mapData = new ConcurrentHashMap<Data, Data>();

    public static void main(String[] args) throws IOException {
           test();
    }

    public static void test() throws IOException {
        final SecureRandom secureRandom = new SecureRandom();
        final int dirName = secureRandom.nextInt();
        final String path = String.valueOf(dirName);
        System.out.println("Filename ---> " + path);
        final HashTable hashTable = new HashTable(path);

        for (int i = 0; i < 800; i++) {

            final Data key = getKey();
            final Data value = getValue();
            hashTable.put(key, value);
            mapData.put(key, value);
        }

        hashTable.close();


        final ReadIndexFile readIndexFile = new ReadIndexFile(path);
        for (Map.Entry<Data, Data> entry : mapData.entrySet()) {
            readIndexFile.getValue(entry.getKey());

        }

         readIndexFile.close();
    }
}
