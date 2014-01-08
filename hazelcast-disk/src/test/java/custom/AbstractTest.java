package custom;

import com.hazelcast.nio.serialization.Data;

import java.security.SecureRandom;

/**
 * @author: ahmetmircik
 * Date: 12/31/13
 */
public abstract class AbstractTest {
    public static final SecureRandom RANDOM = new SecureRandom();

    private static final int K = 1024;

    public static Data getKey() {
        return getKey(16);
    }

    public static Data getValue() {
        return getKey(512);
    }

    public static Data getKey(int size) {
        byte[] pop = new byte[size];
        RANDOM.nextBytes(pop);
        return new Data(0, pop);
    }


    public static Data getValue(int size) {
        byte[] pop = new byte[size];
        RANDOM.nextBytes(pop);
        return new Data(0, pop);
    }

    public static String getDirName(){
        final int dirName = RANDOM.nextInt();
        System.out.println("filename\t:"+dirName);
        return String.valueOf(dirName);
   }

    static byte[] key = {1,2,3};
    static byte[] data = {1,2,3,4,4,5,5,5};

    public static Data getKeyX() {
        return new Data(0, key);
    }

    public static Data getDataX() {
        return new Data(0, data);
    }

}
