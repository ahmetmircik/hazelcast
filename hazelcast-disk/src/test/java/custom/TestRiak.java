package custom;

import com.hazelcast.nio.serialization.Data;

/**
 * @author: ahmetmircik
 * Date: 1/7/14
 */
public class TestRiak extends AbstractTest {

    public static void main(String[] args) {
        final LogStructured riak = new LogStructured(getDirName());

        long wDiff = 0;
        final int size = 100;
        for (int i = 0; i < size; i++) {
            final Data key = getKey(16);
            final Data value = getValue(128);
            long l1 = System.nanoTime();
            riak.put(key, value);

            final Data readVal = riak.get(key);
            if (value.equals(readVal)) {
                System.out.println("ok");
            }
            else {
                System.out.println("not");
            }


            wDiff += System.nanoTime() - l1;
        }


        System.out.println("count-->" + size + " write avg--> " + (wDiff / size));

        riak.close();


    }
}
