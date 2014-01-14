package custom;

import com.hazelcast.disk.Storage;
import com.hazelcast.disk.core.MappedView;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;

/**
 * @author: ahmetmircik
 * Date: 1/5/14
 */
public class TestMMApWrite extends AbstractDiskTest {

    public static void main(String[] args) throws IOException {
        Storage storage = new MappedView(getDirName(), 1<<20);
        final Data key = getKey(1);

        long a = 0;
        for (long i = 0; i < 10L* Integer.MAX_VALUE; i++) {
            storage.writeBytes(a, key.getBuffer());
            a++;
        }


//        a = 0;
//        for (int i = 0; i < 20000; i++) {
//            final byte[] bytes = new byte[3];
//            storage.getBytes(a, bytes);
//            for (int j = 0; j < 3; j++) {
//                if(bytes[j] != key.getBuffer()[j]){
//                    throw new RuntimeException();
//                }
//            }
//            a += 3;
//        }




//        System.out.println(storage.getLong(0));
//        System.out.println(storage.getLong(100));
        storage.close();
    }
}
