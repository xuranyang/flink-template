package com.constant;

public class PropertiesConstants {
    public static final String PRODUCT_ENV = "pro";
    public static final String DEVELOP_ENV = "dev";
    public static final String ENV_TYPE = "env_type";
    public static final String PROPERTIES_FILE_NAME = "/application.properties";
    public static final String GLOBAL_PROPERTIES_FILE_NAME = "/global.application.properties";
    public static final String GLOBAL_DEV_PROPERTIES_FILE_NAME = "/global.application-dev.properties";
    public static final String CHINA_TIME_ZONE = "GMT+08:00";
    public static final String SPLIT_COMMA = ",";
    public static final String SPLIT_COLON = ":";

    public static final String HBASE_PARAM_POOL_SIZE = "hbasePoolSize";
    public static final String HBASE_PARAM_USE_THREAD_POOL = "useThreadPool";
    public static final int HBASE_PARAM_POOL_SIZE_DEFAULT = 5;
    public static final String HBASE_CONNECT_SINGLE = "Single";
    public static final String HBASE_CONNECT_POOL = "Pool";
    public static final long HBASE_WRITE_BUFFER_SIZE = 3 * 1024 * 1024;

    public static final String FLINK_START_PARAMETER_CHK = "chk";
}
