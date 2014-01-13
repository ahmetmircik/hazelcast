package com.hazelcast.disk.core;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;

/**
 * @author: ahmetmircik
 * Date: 12/23/13
 */
public class Main {


    static int globalDepth = 8;

    public static void main(String[] args) throws IOException {


        int x = 0x2;

        System.out.println((byte)0 | x );

//        System.out.println(x += Integer.MAX_VALUE);
//
//        long diff = 0;
//        final int size = (int) Math.pow(2, globalDepth);
//        for (int i = 0; i < size; i++) {
//            final long l = System.nanoTime();
//            newRange0(i, 3);
//            diff += System.nanoTime() - l;
//        }
//
//        long diff2 = 0;
//        for (int i = 0; i < size; i++) {
//            final long l = System.nanoTime();
//            newRange1(i, 3);
//            diff2 += System.nanoTime() - l;
//        }
//
//        System.out.println(diff / size + "....." + diff2 / size);
//
//
//        for (int i = 0; i < size; i++) {
//            final int[] ints = newRange0(i, 1);
//            final int[] ints2 = newRange1(i, 1);
//            if (!Arrays.equals(ints, ints2)) {
//                System.out.println("sdsada");
//            }
//        }

    }

    private static int[] newRange0(int index, final int bucketDepth) {
        final int diff = globalDepth - bucketDepth;
        final String binary = toBinary(index, globalDepth);
        final String substring = reverseFirstBits(binary.substring(diff - 1));
        final Queue<String> strings = new LinkedList<String>();
        strings.add(substring);
        String tmp;
        while ((tmp = strings.peek()) != null && tmp.length() < globalDepth) {
            tmp = strings.poll();
            strings.add("0" + tmp);
            strings.add("1" + tmp);
        }
        final int[] addressList = new int[strings.size()];
        int i = 0;
        while ((tmp = strings.poll()) != null) {
            addressList[i++] = Integer.valueOf(tmp, 2);
        }

        Arrays.sort(addressList);
        return addressList;
    }

    private static String reverseFirstBits(String s) {
        int length = s.length();
        int i = -1;
        String gen = "";
        while (++i < length) {
            int t = Character.digit(s.charAt(i), 2);
            if (i == 0) {
                gen += (t == 0 ? "1" : t);
            } else {
                gen += "" + t;
            }
        }
        return gen;
    }

    /**
     * generate binary prepending by zero.
     */
    private static String toBinary(int hash, int depth) {
        String s = Integer.toBinaryString(hash);
        while (s.length() < depth) {
            s = "0" + s;
        }
        return s;
    }


    private static int[] newRange1(int index, int bucketDepth) {
        ++bucketDepth;
        int lastNBits = ((index & (0xFFFFFFFF >>> (32 - (bucketDepth)))));
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


    static void testWR() throws IOException {
        final Directory directory = new Directory("testWR");
        directory.createFiles();

        // directory.testWrite();
//        directory.loadLastIndexFilePosition();

        directory.close();

    }


}
