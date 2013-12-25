package custom;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.disk.Storage;
import com.hazelcast.disk.impl.Storages;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;

/**
 * @author: ahmetmircik
 * Date: 12/20/13
 */
public class TestDataWrite {

    public static void main(String[] args) throws IOException {
        final HazelcastInstance hzServer = HazelcastInstanceFactory.getHazelcastInstance("test");

        Data key = new Data();
        Data value = new Data();

        Storage storage = Storages.createMMap("test2.dat", 0, 1000);

        storage.put(key, value);


        storage.close();



    }


}
