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
        Storage storage = new MappedView("test33", 1111);
        final Data key = getKey();
        final Data key2 = getKey();

        int y =key.getBuffer().length;
        storage.writeInt(0, key.getBuffer().length);
        storage.writeBytes(4, key.getBuffer());
        storage.writeInt(4 + key.getBuffer().length, key2.getBuffer().length);
        storage.writeBytes(4 + key.getBuffer().length + key2.getBuffer().length, key2.getBuffer());

        int keyLen = storage.getInt(0);

        byte[] bytes = new byte[keyLen];
        storage.getBytes(4,bytes );

        for (int i = 0; i < keyLen; i++) {
            if(bytes[i] != key.getBuffer()[i]){
                System.out.println("asadadsadd");
            }
        }


        System.out.println(storage.getInt(0));
        System.out.println(storage.getInt(4));
        System.out.println(storage.getInt(8));
        storage.close();
    }
}
