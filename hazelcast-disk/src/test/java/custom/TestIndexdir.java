//package custom;
//
//import com.hazelcast.disk.core.IndexDir;
//import com.hazelcast.disk.core.MurmurHash3;
//import com.hazelcast.disk.core.ReadDataFile;
//import com.hazelcast.disk.core.ReadIndexFile;
//import com.hazelcast.nio.serialization.Data;
//
//import java.io.IOException;
//import java.security.SecureRandom;
//import java.util.LinkedList;
//import java.util.Map;
//import java.util.Queue;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.ConcurrentMap;
//import java.util.concurrent.TimeUnit;
//
///**
// * @author: ahmetmircik
// * Date: 12/26/13
// */
//class TestIndexDir extends AbstractTest{
//
//    public static final String TMP = System.getProperty("java.io.tmpdir");
//    public static final String ROOT_DIR = TMP;
//    //+ "/test/exhash/";
//
//    //
//    public static void main(String[] args) throws IOException {
////        System.out.println(makeAddress(638510024, 2));
//        System.out.println(TimeUnit.NANOSECONDS.toMicros(228111));
//        putTest();
////        newRange(6,1);
////        System.out.println(256 * Integer.valueOf("100000000000000011100011",2));
////        System.out.println(Integer.toBinaryString(293));
////        System.out.println(Integer.toBinaryString((int)Math.pow(2,24)));
//
//    }
//
//    static ConcurrentMap<Data, Data> mapData = new ConcurrentHashMap<Data, Data>();
//
//    public static void putTest() throws IOException {
//
//        final SecureRandom secureRandom = new SecureRandom();
//        final int dirName = secureRandom.nextInt();
//        final String path = String.valueOf(dirName);
//        System.out.println("filename==>"+ path);
//        long l = System.nanoTime();
////        final IndexDirBacUp indexDir = new IndexDirBacUp(path);
//        final IndexDir indexDir = new IndexDir(path);
////        indexDir.preallocate(13);
//        long wDiff = 0;
//        System.out.println("prealloca completed");
//        for (int i = 0; i < 100000; i++) {
//
//            final Data key = getKey();
//            final Data value = getValue();
//            long l1 = System.nanoTime();
//            indexDir.insert(key, value);
//            wDiff += System.nanoTime() - l1;
//            mapData.put(key, value);
////            int i1 = secureRandom.nextInt();
////            indexDir.insert(i1,i1);
////            indexDir.insert(0,0);
//        }
//
//        indexDir.close();
//
//
//        long w = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - l);
//        l = System.nanoTime();
//        final ReadIndexFile readIndexFile = new ReadIndexFile(path);
//        long avgCounter = 0;
//        for (Map.Entry<Data, Data> entry : mapData.entrySet()) {
//            long start = System.nanoTime();
//            readIndexFile.getValue(entry.getKey());
//            avgCounter += (System.nanoTime() - start);
//
//        }
//
//        System.out.println("count-->"+mapData.size()+" write avg--> " + (wDiff/mapData.size()));
//        System.out.println("count-->"+mapData.size()+" read avg--> " + (avgCounter/mapData.size()));
//
//        //readIndexFile.print();
//        readIndexFile.close();
//        System.out.println("W-->" + w);
//        System.out.println("R-->" + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - l));
//
////        final ReadDataFile readDataFile = new ReadDataFile(path);
//////        readDataFile.print();
////        readDataFile.close();
//
//
//    }
//
//
//    static int depth = 3;
//
//    static void newRange(int index, int bucketDepth) {
//        final int diff = depth - bucketDepth;
//
//        String s = toBinary(index, depth);
//        String substring = reverseFirstBits(s.substring(diff - 1));
//        Queue<String> strings = new LinkedList<String>();
//        strings.add(substring);
//        String tmp;
//        while ((tmp = strings.peek()) != null && tmp.length() < depth) {
//            tmp = strings.poll();
//            strings.add("0" + tmp);
//            strings.add("1" + tmp);
//        }
//
//        while ((tmp = strings.poll()) != null) {
//            System.out.println(tmp + "-->" + Integer.valueOf(tmp, 2));
//        }
//    }
//
//    static String reverseFirstBits(String s) {
//        int length = s.length();
//        int i = -1;
//        String gen = "";
//        while (++i < length) {
//            int t = Character.digit(s.charAt(i), 2);
//            if (i == 0) {
//                gen += (t == 0 ? "1" : "0");
//            } else {
//                gen += "" + t;
//            }
//        }
//        return gen;
//    }
//
//    static int makeAddress(Integer key, int depth) {
//        if (depth == 0) {
//            return 0;
//        }
//
//        byte[] bytes = (key + "").getBytes();
//        final long hash = MurmurHash3.murmurhash3x8632(bytes, 0, bytes.length, 129);
//        //Arrays.hashCode(key.getBuffer());
//        String s = Long.toBinaryString(hash);
//        //log("s",s);
//        int length = 0;
//        while ((length = s.length()) < depth) {
//            s = "0" + s;
//        }
//        int i = 0;
//        try {
//            i = Integer.parseInt(s.substring(length - depth < 0 ? length : length - depth), 2);
//        } catch (Exception e) {
//            e.printStackTrace();
//
//        }
//        return i >= Integer.MAX_VALUE ? Integer.MAX_VALUE : Math.abs(i);
//    }
//
//
//    static String toBinary(int hash, int depth) {
//        String s = Integer.toBinaryString(hash);
//        while (s.length() < depth) {
//            s = "0" + s;
//        }
//        //log(s);
//        return s;
//    }
//
//    static int findNextBiggerPowerOF2(int i) {
//        int j = 0;
//        while (i >= Math.pow(2, j)) j++;
//        return j;
//    }
//
//
//    public static void getTest() throws IOException {
//        final ReadDataFile readDataFile = new ReadDataFile("126968130_0");
//        readDataFile.print();
//        readDataFile.close();
//
//    }
//
//
//}
