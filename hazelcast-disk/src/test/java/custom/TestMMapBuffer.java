package custom;

import com.hazelcast.nio.serialization.Data;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author: ahmetmircik
 * Date: 1/1/14
 */
public class TestMMapBuffer extends AbstractTest{

    public static void main(String[] args) throws Exception {
        final RandomAccessFile randomAccessFile = new RandomAccessFile("a", "rw");
        final FileChannel channel = randomAccessFile.getChannel();
        final MappedByteBuffer map = channel.map(FileChannel.MapMode.READ_WRITE, 0, (int)Math.pow(2,11));
        final Data key = getKey();

        map.put(key.getBuffer(),0,key.getBuffer().length);
        final int position = map.position();
        map.put(key.getBuffer(),key.getBuffer().length,key.getBuffer().length);
        final int position1 = map.position();
    }
}
