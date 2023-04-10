package com.big.data.engineering3.utils;

public class FileUtils {

    public static String getFileName(String path) {
        int index = path.lastIndexOf('/');
        return path.substring(index + 1);
    }
}
