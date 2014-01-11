//package custom;
//
//import com.hazelcast.disk.core.Hasher;
//import com.hazelcast.nio.serialization.Data;
//
///**
// * @author: ahmetmircik
// * Date: 1/2/14
// */
//public class TestModule extends AbstractTest{
//    public static void main(String[] args) {
//
//        long fs0  = 0;
//        long fs1  = 0;
//
//        int size =100000;
//
//        for (int i = 0; i < size; i++) {
//            final Data key1 = getKey();
//            final long l = System.nanoTime();
//            findSlot0(key1, 1);
//            findSlot0(key1, 2);
//            findSlot0(key1, 3);
//            findSlot0(key1, 4);
//            findSlot0(key1, 5);
//            findSlot0(key1, 6);
//            findSlot0(key1, 7);
//            findSlot0(key1, 8);
//            fs0 += System.nanoTime()-l;
//
//            final long l1 = System.nanoTime();
//            findSlot1(key1, 1);
//            findSlot1(key1, 2);
//            findSlot1(key1, 3);
//            findSlot1(key1, 4);
//            findSlot1(key1, 5);
//            findSlot1(key1, 6);
//            findSlot1(key1, 7);
//            findSlot1(key1, 8);
//            fs1 += System.nanoTime()-l1;
//        }
//
//        System.out.println(fs0 / size + " " + fs1 / size);
//    }
//
//    private static final Hasher<Data, Integer> HASHER = Hasher.DATA_HASHER;
//
//    static int findSlot0(Data key, int depth) {
//        if (depth == 0) {
//            return 0;
//        }
//        final int hash = HASHER.hash(key);
//        return ((hash & (0xFFFFFFFF >>> (32 - depth))));
//
//    }
//
//    static int findSlot1(Data key, int depth) {
//        if (depth == 0) {
//            return 0;
//        }
//        long hash = HASHER.hash(key);
//
//        String s = Long.toBinaryString(hash);
//        int length;
//        while ((length = s.length()) < depth) {
//            s = "0" + s;
//        }
//        int i;
//        try {
//            i = Integer.parseInt(s.substring(length - depth < 0 ? length : length - depth), 2);
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//
//        }
//        return i;
//    }
//}
