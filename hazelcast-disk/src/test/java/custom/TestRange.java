package custom;

import java.util.Arrays;

/**
 * @author: ahmetmircik
 * Date: 1/13/14
 */
public class TestRange  {

    public static void main(String[] args) {

        final int[] ints = newRange2(0, 1);
        for (int i = 0; i < ints.length; i++) {
            System.out.println(ints[i]);
        }
    }


    static int globalDepth = 2;
    static int[] newRange2(int index, int bucketDepth) {
        ++bucketDepth;
        int lastNBits = ((index & (0x7FFFFFFF >>> (32 - (bucketDepth)))));
        lastNBits |= (int) Math.pow(2, bucketDepth - 1);

        int[] x = new int[(int) Math.pow(2, globalDepth - bucketDepth)];
        for (int i = 0; i < x.length; i++) {
            x[i] = -1;
        }
        x[0] = lastNBits;

        while (globalDepth - bucketDepth > 0) {
            final int mask = (int) Math.pow(2, bucketDepth);
            final int pow = (int) Math.pow(2, globalDepth - bucketDepth - 1);
            for (int i = 0; i < x.length; i+=pow) {
                if (x[i] == -1) continue;
                final Integer param = x[i];
                x[i] = (param | mask);
                x[i+=pow] = (param & ~mask);
            }

            bucketDepth++;
        }
        Arrays.sort(x);
        return x;
    }

}
