package com.util;


import com.constant.PropertiesConstants;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class CommonUtils {
    public static String getCurrentTime() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf.format(new Date());
    }

    // GMT+02:00
    public static String getCurrentTime(String timeZone) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sdf.setTimeZone(TimeZone.getTimeZone(timeZone));
        return sdf.format(new Date());
    }

    public static String getGlobalPropertiesFileName() {
        return PropertiesConstants.GLOBAL_PROPERTIES_FILE_NAME;
    }


    public static String getGlobalPropertiesFileName(String propType) {
        String propertiesFile = PropertiesConstants.GLOBAL_PROPERTIES_FILE_NAME;
        if (PropertiesConstants.PRODUCT_ENV.equalsIgnoreCase(propType)) {
            propertiesFile = PropertiesConstants.GLOBAL_PROPERTIES_FILE_NAME;
        } else if (PropertiesConstants.DEVELOP_ENV.equalsIgnoreCase(propType)) {
            propertiesFile = PropertiesConstants.GLOBAL_DEV_PROPERTIES_FILE_NAME;
        }

        return propertiesFile;
    }

    public static <T> String ifNull(T obj) {
        if (obj == null) {
            return "\\N";
        } else {
            return obj.toString();
        }
    }
}

