package com.hazelcast.disk;

/**
 * @author: ahmetmircik
 * Date: 1/9/14
 */
public final class Utils {

    public static int next2(int number){
        int i = 0;
        int result;
        while ( (result = (int)Math.pow(2,i)) < number ){
            i++;
        }
        return result;
    }


    public static int numberOfTwos(int number){
        int i = 0;
        while ( (number = (number >>> 1)) != 0 ){
            i++;
        }
        return i;
    }


}
