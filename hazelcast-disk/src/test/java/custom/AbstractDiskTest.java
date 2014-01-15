package custom;

import com.hazelcast.disk.core.Hashtable;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.util.ConstructorFunction;

import java.security.SecureRandom;

/**
 * @author: ahmetmircik
 * Date: 12/31/13
 */
public abstract class AbstractDiskTest extends HazelcastTestSupport {

    protected static final SecureRandom RANDOM = new SecureRandom();

    public static final String TEMP_DIR = System.getProperty("java.io.tmpdir");

    private static final int K = 1024;

    public static Data getKey() {
        return getData(8);
    }

    public static Data getValue() {
        return getData(16);
    }

    public static Data getKey(int size) {
        return getData(size);
    }


    public static Data getValue(int size) {
        return getData(size);
    }

    public static Data getData(int size) {
        byte[] pop = new byte[size];
        RANDOM.nextBytes(pop);
        return new Data(0, pop);
    }


    public static Data getData(int sizeMin, int sizeMax) {
        int size = RANDOM.nextInt(sizeMax);
        if(size <  sizeMin ) size =sizeMin;
        return getData(size);
    }

    public static String getDirName(){
        final int dirName = RANDOM.nextInt();
        String path = TEMP_DIR +"" + String.valueOf(dirName);
        System.out.println("--->Path\t:"+path);
        return path;
    }

    static byte[] key = {1,2,3};
    static byte[] data = {1,2,3,4,4,5,5,5};

    public static Data getKeyX() {
        return new Data(0, key);
    }

    public static Data getDataX() {
        return new Data(0, data);
    }

    public static Hashtable<Data,Data> getHashTable(String path){
        final Hashtable<Data,Data> hashtable = new Hashtable<Data,Data>(path);
        hashtable.setKeyConstructorFunction(new ConstructorFunction<byte[], Data>() {
            @Override
            public Data createNew(byte[] arg) {
                return new Data(0,arg);
            }
        });
        hashtable.setValueConstructorFunction(new ConstructorFunction<byte[], Data>() {
            @Override
            public Data createNew(byte[] arg) {
                return new Data(0,arg);
            }
        });

        return hashtable;
    }

}
