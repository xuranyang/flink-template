package com.sink.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropUtil {
    public Properties getProp() {
        Properties properties = new Properties();

        InputStream inputStream = this.getClass().getResourceAsStream("/application.properties");
        try {
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return properties;

    }

//    public static void main(String[] args) {
//        PropUtil propUtil = new PropUtil();
//        Properties prop = propUtil.getProp();
//        String property = prop.getProperty("hbase.zookeeper.quorum");
//        System.out.println(property);
//    }
}
