package com.source.utils;

public class FlinkUtils {
    public <T> String ifNullToString(T data) {
        if (data == null) {
            return "";
        } else {
            return data.toString();
        }
    }

}
