package com.source.utils;

import java.util.List;

public class CommonUtils {
    public static <T> String ifNullToString(T data) {
        if (data == null) {
            return "";
        } else {
            return data.toString();
        }
    }

    public static <T> T getListFisrt(List<T> data) {
        if (data == null || data.size() == 0) {
            return null;
        }
        return data.get(0);
    }

    public static <T> List<T> getListTopN(List<T> data, int topN) {
        return data.subList(0, topN);
    }

}
