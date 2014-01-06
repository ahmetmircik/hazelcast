package custom;

import com.hazelcast.nio.serialization.Data;

import java.security.SecureRandom;

/**
 * @author: ahmetmircik
 * Date: 12/31/13
 */
public abstract class AbstractTest {
    public static final SecureRandom RANDOM = new SecureRandom();

    public static Data getKey() {
        byte[] pop = new byte[72];
        RANDOM.nextBytes(pop);
        return new Data(0, pop);
    }

    public static Data getValue() {
        byte[] pop = new byte[3000];
        RANDOM.nextBytes(pop);
        return new Data(0, pop);
    }

    public static String getDirName(){
        final int dirName = RANDOM.nextInt();
        return String.valueOf(dirName);
   }

}
