//package custom;
//
//import com.hazelcast.disk.core.Hasher;
//import com.hazelcast.disk.helper.SipHashInline;
//
//import java.util.HashSet;
//import java.util.Set;
//
///**
// * @author: ahmetmircik
// * Date: 1/11/14
// */
//public class TestHash extends AbstractTest{
//
//    static long k0 = 0x0706050403020100L;
//    static long k1 = 0x0f0e0d0c0b0a0908L;
//
//    //          8 byte        16 byte     32          128         512 byte
//    // sip :    193843000     286542000   4395706000  16800473000 71311209000
//    // mrmur :  204098000     305405000   4310583000  19310445000 70439580000
//
//    static Set<Integer> set = new HashSet<Integer>();
//    static Set<Long> set2 = new HashSet<Long>();
//    public static void main(String[] args) {
//
//        for (int i = 0; i < 1000; i++) {
//            System.out.println(Long.toBinaryString(SipHashInline.hash24(k0, k1, getKey(128).getBuffer())));
//
//        }
////        tt();
//
//    }
//
//    static void tt(){
//        final long l = System.nanoTime();
//        for (int i = 0; i < 1000000; i++) {
//            Hasher.DATA_HASHER.hash(getKey(128));
////            SipHashInline.hash24(k0, k1, getKey(128).getBuffer());
////
//        }
//        System.out.println(System.nanoTime() - l);
//    }
//
//    public static void diff() {
//
//        final long l = System.nanoTime();
//        for (int i = 0; i < 500000; i++) {
//            set.add(Hasher.DATA_HASHER.hash(getKey(300)));
//            set2.add(SipHashInline.hash24(k0, k1, getKey(300).getBuffer()));
//
//        }
//        System.out.println(set.size() +"==="+ set2.size());
//    }
//
//}
