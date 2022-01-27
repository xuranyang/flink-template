package com.sink.util;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class PropUtil {
    ParameterTool parameterTool;

    public PropUtil() {
        try {
            parameterTool = ParameterTool.fromPropertiesFile(this.getClass().getResourceAsStream("/application.properties"));
        } catch (IOException e) {
            System.out.println("获取配置文件路径异常:" + e.getMessage());
        }
    }

    public PropUtil(String path) {
        if (!path.startsWith("/")) {
            path = "/" + path;
        }

        try {
            parameterTool = ParameterTool.fromPropertiesFile(this.getClass().getResourceAsStream(path));
        } catch (IOException e) {
            System.out.println("获取配置文件路径异常:" + e.getMessage());
        }
    }

    public String getJsonProp(String fileName) {
        if (!fileName.startsWith("/")) {
            fileName = "/" + fileName;
        }

        try {
            InputStream inputStream = this.getClass().getResourceAsStream(fileName);
            if (inputStream != null) {
                StringBuilder sb = new StringBuilder();
                InputStreamReader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
                BufferedReader bfReader = new BufferedReader(reader);
                String content = null;
                while ((content = bfReader.readLine()) != null) {
                    sb.append(content);
                }
                bfReader.close();
                return sb.toString();
            } else {
                return null;
            }

        } catch (Exception e) {
            return null;
        }

    }

    public String getStringProp(String key) {
        return parameterTool.get(key);
    }

    public int getIntProp(String key) {
        return parameterTool.getInt(key);
    }

    public ParameterTool getParameterTool() {
        return parameterTool;
    }

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
