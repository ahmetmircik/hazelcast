package com.hazelcast.map.impl.recordstore.rocks;

import java.util.Arrays;

import static com.hazelcast.internal.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.util.Preconditions.checkPositive;

/**
 * Utility methods for padding string and integer.
 */
public final class PaddingUtil {

    private static final char ZERO = '0';

    private PaddingUtil() {
    }

    /**
     * @param number to be padded
     * @return padded number as string
     */
    public static String leftPadUpToTenWithZero(int number) {
        checkNotNegative(number, "number cannot be negative");

        int size = stringSize(number);
        int diff = PadLengths.CACHE.length - size;
        if (diff >= 0) {
            return PadLengths.CACHE[diff] + number;
        }

        return String.valueOf(number);
    }

    /**
     * @param str    string subject to padding
     * @param maxPad length of the padded string
     * @return padded string
     */
    public static String leftPadWithZero(String str, int maxPad) {
        checkPositive("maxPad", maxPad);

        int size = str.length();
        if (size > maxPad) {
            return str;
        }

        int length = Math.min(PadLengths.CACHE.length, maxPad);
        int diff = length - size;
        if (diff > 0) {
            return PadLengths.CACHE[diff - 1] + str;
        }

        StringBuilder sb = new StringBuilder(size);
        char[] tmp = new char[-diff];
        Arrays.fill(tmp, ZERO);

        return sb.append(tmp).append(str).toString();
    }

    /**
     * Copied from {@link Integer#stringSize}
     * <p>
     *
     * @return string representation size for a given int value.
     */
    @SuppressWarnings("checkstyle:MagicNumber")
    private static int stringSize(int x) {
        int d = 1;
        if (x >= 0) {
            d = 0;
            x = -x;
        }
        int p = -10;
        for (int i = 1; i < 10; i++) {
            if (x > p) {
                return i + d;
            }
            p = 10 * p;
        }
        return 10 + d;
    }

    private static final class PadLengths {
        private static final String ONE = "0";
        private static final String TWO = ONE + ONE;
        private static final String TREE = TWO + ONE;
        private static final String FOUR = TREE + ONE;
        private static final String FIVE = FOUR + ONE;
        private static final String SIX = FIVE + ONE;
        private static final String SEVEN = SIX + ONE;
        private static final String EIGHT = SEVEN + ONE;
        private static final String NINE = EIGHT + ONE;

        private static final String[] CACHE = {ONE, TWO, TREE, FOUR,
                FIVE, SIX, SEVEN, EIGHT, NINE};
    }
}
