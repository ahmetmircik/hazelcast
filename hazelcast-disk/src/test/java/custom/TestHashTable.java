//package custom;
//
//import com.hazelcast.disk.core.HashTable;
//import com.hazelcast.disk.core.Hasher;
//import com.hazelcast.nio.serialization.Data;
//
//import java.io.IOException;
//import java.util.Map;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.ConcurrentMap;
//import java.util.concurrent.TimeUnit;
//
///**
// * @author: ahmetmircik
// * Date: 1/1/14
// */
//public class TestHashTable extends AbstractDiskTest {
//
//    private static ConcurrentMap<Data, Data> mapData = new ConcurrentHashMap<Data, Data>();
//
//    private static final Hasher<Data, Integer> HASHER = Hasher.DATA_HASHER;
//
//
//    public static void main(String[] args) throws IOException {
//
//        final String path = getDirName();
//        test(path);
////        compare(path);
////        read(path);
////        readCompare(path);
//
////        System.out.println(TimeUnit.NANOSECONDS.toSeconds(171070314000L));
//
//    }
////count-->50000000 write avg--> 3241
//
//    public static void test(String path) throws IOException {
//
//        final HashTable hashTable = new HashTable(path);
//        long wDiff = 0;
//        final int size = 1000;
//        for (int i = 0; i < size; i++) {
//            final Data key = getKey(8);
//            final Data value = getValue(10);
//            long l1 = System.nanoTime();
//            hashTable.put(key, value);
//            wDiff += System.nanoTime() - l1;
//            mapData.put(key, value);
//        }
//
//        for (Map.Entry<Data, Data> entry : mapData.entrySet()) {
//            if (!entry.getValue().equals(hashTable.remove(entry.getKey())) )
//            {
//                System.out.println("remove problem");
//            }
//        }
//
//
//        for (int i = 0; i < 1; i++) {
//            final Data key = getKey(8);
//            final Data value = getValue(10);
//            long l1 = System.nanoTime();
//            hashTable.put(key, value);
//            wDiff += System.nanoTime() - l1;
//            mapData.put(key, value);
//        }
//
////        int i = 0;
////        for (Map.Entry<Data, Data> entry : mapData.entrySet()) {
////            if (entry.getValue().equals(hashTable.remove(entry.getKey())) )
////            {
////                System.out.println("removed success. hash -->"+entry.getKey().hashCode()+" " + (hashTable.remove(entry.getKey()) == null));
////            }
////            i++;
////
////            if (i == 2)break;
////        }
////
////
////        for (Map.Entry<Data, Data> entry : mapData.entrySet()) {
////            if (!entry.getValue().equals(hashTable.get(entry.getKey())) )
////            {
////                System.out.println("get failed\t key hash --->" + entry.getKey().hashCode());
////            }
////
////        }
//
//
//        System.out.println("count-->" + size + " write avg--> " + (wDiff / size));
//
//        hashTable.close();
//        System.out.println("closed");
//    }
//
//    public static void compare(String path) throws IOException {
//        final HashTable hashTable = new HashTable(path);
//        long avgCounter = 0;
//        for (Map.Entry<Data, Data> entry : mapData.entrySet()) {
//            long start = System.nanoTime();
//            final Data data = hashTable.get(entry.getKey());
//            final Data value = entry.getValue();
//            try {
//                if (!value.equals(data)) {
//                    System.out.println("[" + entry.getKey().hashCode() + "] should be equal...");
//                }
//
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//            avgCounter += (System.nanoTime() - start);
//
//        }
//        System.out.println("count-->" + mapData.size() + " read avg--> " + (avgCounter / mapData.size()));
//
//        hashTable.close();
//
//    }
//
////    public static void readCompare(String path) {
////        final ReadIndexFile readIndexFile = new ReadIndexFile(path);
////        long avgCounter = 0;
////        for (Map.Entry<Data, Data> entry : mapData.entrySet()) {
////            long start = System.nanoTime();
////            final Data data = readIndexFile.getData(entry.getKey());
////            final Data value = entry.getValue();
////            try {
////                if (!value.equals(data)) {
////                    throw new RuntimeException("[" + entry.getKey().hashCode() + "] should be equal...");
////                }
////
////            } catch (Exception e) {
////                e.printStackTrace();
////            }
////            avgCounter += (System.nanoTime() - start);
////
////        }
////        System.out.println("count-->" + mapData.size() + " read avg--> " + (avgCounter / mapData.size()));
////
////        readIndexFile.close();
////
////    }
//
//    public static void read(String path) throws IOException {
//
//        final HashTable readIndexFile = new HashTable(path);
//        long l1 = System.nanoTime();
//        final Map<Data, Data> dataDataMap = readIndexFile.loadAll();
//        for (Map.Entry<Data, Data> entry : mapData.entrySet()) {
//            long start = System.nanoTime();
//            final Data data = dataDataMap.get(entry.getKey());
//            long diff = System.nanoTime() - start;
//            l1 += diff;
//
//            final Data value = entry.getValue();
//            try {
//                if (!value.equals(data)) {
//                    throw new RuntimeException("[" + entry.getKey().hashCode() + "] should be equal...");
//                }
//
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//
//        }
////        System.out.   <></>println("key count " + readIndexFile.keyCount);
//        System.out.println("Total time\t: " + (System.nanoTime() - l1));
//
//        final long count = readIndexFile.size();
//        final long l2 = System.nanoTime() - l1;
//        final long l = l2 / count;
//        System.out.println(l2 + " " + TimeUnit.NANOSECONDS.toMillis(l2));
//        System.out.println("count-->" + count + " read avg--> " + l);
//
//
//        readIndexFile.close();
//    }
//}
