package com.hazelcast.disk.helper;

import java.io.File;

/**
 * @author: ahmetmircik
 * Date: 1/14/14
 */
public enum  FileHelper {
    ;

    // for testing
    public static void deleteOnExit(String basePath) {
        for (String name : new String[]{basePath + ".data", basePath + ".index"}) {
            File file = new File(name);
            file.delete();
            file.deleteOnExit();
        }
    }
}
