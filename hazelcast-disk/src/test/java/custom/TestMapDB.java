package custom;

import com.hazelcast.nio.serialization.Data;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.File;
import java.security.SecureRandom;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author: ahmetmircik
 * Date: 12/31/13
 */
public class TestMapDB extends AbstractTest{

    public static void main(String[] args) throws Exception {
           test();
    }

    static ConcurrentMap<Data, Data> mapData = new ConcurrentHashMap<Data, Data>();

//    count-->100000 write avg--> 10805
//    count-->100000 read avg--> 400
    public static void test() {
        final SecureRandom secureRandom = new SecureRandom();
        final int dirName = secureRandom.nextInt();
        final String path = String.valueOf(dirName);
        DB db = DBMaker.newFileDB(new File(path)).closeOnJvmShutdown().make();
        Map<Data, Data> map = db.getHashMap(getDirName());
        long wDiff = 0;
        int size = 20000;
        for (int i = 0; i < size; i++) {
            final Data key = getKey();
            final Data value = getValue();
            long l1 = System.nanoTime();
            map.put(key,value);
            wDiff += System.nanoTime() - l1;
//            mapData.put(key,value);
        }
        db.commit();
        db.close();


//        db = DBMaker.newFileDB(new File(path)).closeOnJvmShutdown().make();
//        map = db.getHashMap("test6");
//        long avgCounter = 0;
//        for (Map.Entry<Data, Data> entry : mapData.entrySet()) {
//            long start = System.nanoTime();
//            map.get(entry.getKey());
//            avgCounter += (System.nanoTime() - start);
//
//        }
//        db.commit();
//        db.close();

        System.out.println("count-->"+size+" write avg--> " + (wDiff/size));
//        System.out.println("count-->"+mapData.size()+" read avg--> " + (avgCounter/mapData.size()));



    }
}
