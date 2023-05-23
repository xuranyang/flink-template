package com.util.hbase;

import com.constant.PropertiesConstants;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HbaseAutoConfiguration {
    private HbaseProperties hbaseProperties;

    private static final Logger LOGGER = LoggerFactory.getLogger(HbaseAutoConfiguration.class);

    private static final String HBASE_QUORUM = "hbase.zookeeper.quorum";
    private static final String HBASE_ZK_PORT = "hbase.zookeeper.property.clientPort";

    private static final String HBASE_ROOTDIR = "hbase.rootdir";
    private static final String HBASE_ZNODE_PARENT = "zookeeper.znode.parent";
    private static final String HBASE_RETRY_NUM = "hbase.client.retries.number";

    public HbaseAutoConfiguration(HbaseProperties hbaseProperties) {
        this.hbaseProperties = hbaseProperties;
    }

    public HbaseUtils hbaseTemplate() {
        Configuration configuration = HBaseConfiguration.create();
//        configuration.set(HBASE_QUORUM, hbaseProperties.getZkQuorum());
//        configuration.set(HBASE_ZK_PORT, String.valueOf(hbaseProperties.getZkPort()));
//        configuration.set(HBASE_ROOTDIR, hbaseProperties.getRootDir());
//        configuration.set(HBASE_ZNODE_PARENT, hbaseProperties.getZNodeParent());
//        configuration.set(HBASE_RETRY_NUM, String.valueOf(hbaseProperties.getRetryNum()));
//        configuration.set(PropertiesConstants.HBASE_PARAM_POOL_SIZE, String.valueOf(hbaseProperties.getPoolSize()));

        setHbaseConfigNotBlank(HBASE_QUORUM, hbaseProperties.getZkQuorum(), configuration);
        setHbaseConfigNotBlank(HBASE_ZK_PORT, String.valueOf(hbaseProperties.getZkPort()), configuration);
        setHbaseConfigNotBlank(HBASE_ROOTDIR, hbaseProperties.getRootDir(), configuration);
        setHbaseConfigNotBlank(HBASE_ZNODE_PARENT, hbaseProperties.getZNodeParent(), configuration);
        setHbaseConfigNotBlank(HBASE_RETRY_NUM, String.valueOf(hbaseProperties.getRetryNum()), configuration);
        setHbaseConfigNotBlank(PropertiesConstants.HBASE_PARAM_POOL_SIZE, String.valueOf(hbaseProperties.getPoolSize()), configuration);
        setHbaseConfigNotBlank(PropertiesConstants.HBASE_PARAM_USE_THREAD_POOL, String.valueOf(hbaseProperties.getUseThreadPool()), configuration);

        LOGGER.info("======[HbaseAutoConfiguration Init]: HBaseTemplate Configs={}", hbaseProperties);
        LOGGER.info("======[Hbase Configuration]: HBaseConfiguration={}", configuration);
        return new HbaseUtils(configuration);
    }

    public void setHbaseConfigNotBlank(String configName, String configValue, Configuration configuration) {
        if (StringUtils.isNotBlank(configValue)) {
            configuration.set(configName, configValue);
        }
    }
}
