package com.hazelcast.disk.helper;

/**
 * @author: ahmetmircik
 * Date: 1/9/14
 */
public enum Utils {
    ;

    public static int nextPowerOf2(int number){
        int i = 0;
        int result;
        while ( (result = (int)Math.pow(2,i)) < number ){
            i++;
        }
        return result;
    }


    public static int numberOf2s(int number){
        int i = 0;
        while ( (number = (number >>> 1)) != 0 ){
            i++;
        }
        return i;
    }


}
