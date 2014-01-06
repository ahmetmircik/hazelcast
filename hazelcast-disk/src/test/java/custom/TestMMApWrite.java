package custom;

import com.hazelcast.disk.Storage;
import com.hazelcast.disk.core.MappedView;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;

/**
 * @author: ahmetmircik
 * Date: 1/5/14
 */
public class TestMMApWrite extends AbstractTest {

    public static void main(String[] args) throws IOException {
        Storage storage = new MappedView(getDirName(), 1<<7);
        final Data key = getKey();

        long a = 0;
        for (int i = 0; i < 20000; i++) {
            storage.writeBytes(a, key.getBuffer());
            a += 512;
        }


        a = 0;
        for (int i = 0; i < 20000; i++) {
            final byte[] bytes = new byte[512];
            storage.getBytes(a, bytes);
            for (int j = 0; j < 512; j++) {
                if(bytes[j] != key.getBuffer()[j]){
                    throw new RuntimeException();
                }
            }
            a += 512;
        }




//        System.out.println(storage.getLong(0));
//        System.out.println(storage.getLong(100));
        storage.close();
    }
}
