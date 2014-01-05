package custom;

/**
 * @author: ahmetmircik
 * Date: 1/2/14
 */
public class TestModule {
    public static void main(String[] args) {

        final int mod = 102 >>>5;

        System.out.println(mod);

        final int i = ((int) Math.pow(2, 30)) - 1;

        System.out.println("-->"+Long.toBinaryString((long)Math.pow(2,30) -1));
        System.out.println("-->"+Long.toBinaryString((long)Math.pow(2,35)));
        final long pow = (long) Math.pow(2, 35);

        System.out.println(pow & i);

        System.out.println(Long.toBinaryString((long)Math.pow(2,30) -1));
        System.out.println(Long.numberOfLeadingZeros((long)Math.pow(2,30)-1));
    }
}
