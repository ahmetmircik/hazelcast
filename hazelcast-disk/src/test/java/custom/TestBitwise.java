package custom;

import java.security.SecureRandom;

/**
 * @author: ahmetmircik
 * Date: 1/9/14
 */
public class TestBitwise {

    public static void main(String[] args) {
        final int viewIndex = (int) (8192L / (int)Math.pow(2, 16));
        System.out.println(viewIndex);
    }

    static SecureRandom secureRandom = new SecureRandom();
    static void test(){
        final long l = System.nanoTime();
        final int size = 32;
        for (int i = 0; i < size; i++) {
             long l1 = secureRandom.nextLong();
            if(l1<0)l1 = -l1;
            final long i1 = 1 / (long) Math.pow(2, 12);
            final long i2 = 1 >>> 12;
            if(i1 != i2){
                System.out.println("asxsaasa");
            }
        }
        System.out.println((System.nanoTime() - l )/ size);

        final long l1 = System.nanoTime();
        for (int i = 0; i < size; i++) {
            final int i1 = (int) Math.pow(2, 43) >>> 30;
        }
        System.out.println((System.nanoTime() - l1 )/ size);
    }


}
